/*
 *  cam_ascom_alpaca.cpp
 *  PHD Guiding
 *
 *  Created by Nico Trost
 *  Copyright (c) 2026 Nico Trost
 *  All rights reserved.
 *
 *  This source code is distributed under the following "BSD" license
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *    Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *    Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *    Neither the name of openphdguiding.org nor the names of its
 *     contributors may be used to endorse or promote products derived from
 *     this software without specific prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include "phd.h"

#if defined(ALPACA_CAMERA)

# include "cam_ascom_alpaca.h"
# include "camera.h"
# include "image_math.h"
# include "json_parser.h"
# include "worker_thread.h"

# include <climits>
# include <cstdint>
# include <cstdlib>
# include <cstring>
# include <memory>
# include <string>
# include <utility>
# include <vector>

# include <curl/curl.h>
# include <wx/socket.h>

# if defined(__WINDOWS__)
#  include <winsock2.h>
# else
#  include <sys/socket.h>
# endif

// ==========================================================================
// small JSON helpers
// ==========================================================================

static const json_value *FindMember(const json_value *obj, const char *name)
{
    if (!obj)
        return nullptr;
    json_for_each (item, obj)
    {
        if (item->name && strcmp(item->name, name) == 0)
            return item;
    }
    return nullptr;
}

static bool JsonAsBool(const json_value *v, bool defaultVal = false)
{
    if (!v)
        return defaultVal;
    if (v->type == JSON_BOOL)
        return v->int_value != 0;
    return defaultVal;
}

static int JsonAsInt(const json_value *v, int defaultVal = 0)
{
    if (!v)
        return defaultVal;
    if (v->type == JSON_INT)
        return v->int_value;
    if (v->type == JSON_FLOAT)
        return (int) v->float_value;
    return defaultVal;
}

static double JsonAsDouble(const json_value *v, double defaultVal = 0.0)
{
    if (!v)
        return defaultVal;
    if (v->type == JSON_FLOAT)
        return v->float_value;
    if (v->type == JSON_INT)
        return v->int_value;
    return defaultVal;
}

static wxString JsonAsString(const json_value *v, const wxString& defaultVal = wxEmptyString)
{
    if (!v || v->type != JSON_STRING || !v->string_value)
        return defaultVal;
    return wxString::FromUTF8(v->string_value);
}

static size_t AlpacaWriteCallback(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    std::string *buf = static_cast<std::string *>(userdata);
    buf->append(ptr, size * nmemb);
    return size * nmemb;
}

// ==========================================================================
// AlpacaClient -- a tiny REST client scoped to a single Alpaca camera device
// (http://{host}:{port}/api/v1/camera/{devicenumber}/{action}). Kept private
// to this file since PHD2 only needs Alpaca for cameras today (see plan).
// ==========================================================================

class AlpacaClient
{
    CURL *m_curl;
    curl_slist *m_headers;
    wxString m_host;
    int m_port;
    int m_deviceNumber;
    unsigned int m_clientId;
    unsigned int m_transactionId;
    int m_timeoutMs;

    wxString BaseUrl(const wxString& action) const
    {
        return wxString::Format("http://%s:%d/api/v1/camera/%d/%s", m_host, m_port, m_deviceNumber, action);
    }

    wxString Escape(const wxString& s) const
    {
        char *escaped = curl_easy_escape(m_curl, static_cast<const char *>(s.c_str()), 0);
        wxString result = escaped ? wxString::FromUTF8(escaped) : wxString();
        if (escaped)
            curl_free(escaped);
        return result;
    }

    bool Perform(const wxString& url, const wxString *putBody, std::string *response, wxString *err)
    {
        if (!m_curl)
        {
            *err = _("Alpaca: HTTP client not initialized");
            return true;
        }

        response->clear();
        curl_easy_setopt(m_curl, CURLOPT_URL, static_cast<const char *>(url.c_str()));
        curl_easy_setopt(m_curl, CURLOPT_WRITEDATA, response);
        curl_easy_setopt(m_curl, CURLOPT_TIMEOUT_MS, (long) m_timeoutMs);

        if (putBody)
        {
            curl_easy_setopt(m_curl, CURLOPT_CUSTOMREQUEST, "PUT");
            curl_easy_setopt(m_curl, CURLOPT_POSTFIELDS, static_cast<const char *>(putBody->c_str()));
            curl_easy_setopt(m_curl, CURLOPT_POSTFIELDSIZE, (long) putBody->length());
        }
        else
        {
            curl_easy_setopt(m_curl, CURLOPT_CUSTOMREQUEST, (const char *) nullptr);
            curl_easy_setopt(m_curl, CURLOPT_POSTFIELDS, (const char *) nullptr);
            curl_easy_setopt(m_curl, CURLOPT_HTTPGET, 1L);
        }

        CURLcode res = curl_easy_perform(m_curl);
        if (res != CURLE_OK)
        {
            *err = wxString::Format(_("Alpaca HTTP error: %s"), curl_easy_strerror(res));
            Debug.Write(wxString::Format("Alpaca: request to %s failed: %s\n", url, curl_easy_strerror(res)));
            return true;
        }

        long httpCode = 0;
        curl_easy_getinfo(m_curl, CURLINFO_RESPONSE_CODE, &httpCode);
        if (httpCode < 200 || httpCode >= 300)
        {
            *err = wxString::Format(_("Alpaca server returned HTTP %ld"), httpCode);
            Debug.Write(wxString::Format("Alpaca: %s returned HTTP %ld: %s\n", url, httpCode, *response));
            return true;
        }

        return false;
    }

    // Parses the standard Alpaca response envelope {Value, ErrorNumber, ErrorMessage, ...}.
    // *value is set to the "Value" member (may remain null for actions with no return value).
    bool ParseEnvelope(const std::string& response, JsonParser *parser, const json_value **value, wxString *err)
    {
        *value = nullptr;

        if (!parser->Parse(response))
        {
            *err = wxString::Format(_("Alpaca: invalid JSON response (%s)"), parser->ErrorDesc());
            Debug.Write(wxString::Format("Alpaca: JSON parse error: %s\n", parser->ErrorDesc()));
            return true;
        }

        const json_value *root = parser->Root();
        int errNum = JsonAsInt(FindMember(root, "ErrorNumber"), 0);
        if (errNum != 0)
        {
            wxString msg = JsonAsString(FindMember(root, "ErrorMessage"));
            *err = wxString::Format(_("Alpaca driver error %d: %s"), errNum, msg);
            return true;
        }

        *value = FindMember(root, "Value");
        return false;
    }

public:
    AlpacaClient(const wxString& host, int port, int deviceNumber)
        : m_curl(curl_easy_init()), m_headers(nullptr), m_host(host), m_port(port), m_deviceNumber(deviceNumber),
          m_clientId(1000u + (unsigned int) (rand() % 9000)), m_transactionId(0), m_timeoutMs(5000)
    {
        if (m_curl)
        {
            curl_easy_setopt(m_curl, CURLOPT_USERAGENT, static_cast<const char *>(wxGetApp().UserAgent().c_str()));
            curl_easy_setopt(m_curl, CURLOPT_WRITEFUNCTION, AlpacaWriteCallback);
            curl_easy_setopt(m_curl, CURLOPT_NOSIGNAL, 1L);

            // Force the classic JSON response format. Without an explicit
            // Accept header, some Alpaca servers (notably ImageArray) default
            // to the efficient binary ImageBytes format instead, which this
            // client does not implement (see cam_ascom_alpaca plan, Phase 3).
            m_headers = curl_slist_append(m_headers, "Accept: application/json");
            curl_easy_setopt(m_curl, CURLOPT_HTTPHEADER, m_headers);
        }
    }

    ~AlpacaClient()
    {
        if (m_curl)
            curl_easy_cleanup(m_curl);
        if (m_headers)
            curl_slist_free_all(m_headers);
    }

    bool IsOk() const { return m_curl != nullptr; }

    void SetTimeoutMs(int ms) { m_timeoutMs = ms; }

    bool Get(const wxString& action, JsonParser *parser, const json_value **value, wxString *err)
    {
        ++m_transactionId;
        wxString url = wxString::Format("%s?ClientID=%u&ClientTransactionID=%u", BaseUrl(action), m_clientId, m_transactionId);

        std::string response;
        if (Perform(url, nullptr, &response, err))
            return true;

        return ParseEnvelope(response, parser, value, err);
    }

    bool Put(const wxString& action, const std::vector<std::pair<wxString, wxString>>& params, wxString *err)
    {
        ++m_transactionId;
        wxString body = wxString::Format("ClientID=%u&ClientTransactionID=%u", m_clientId, m_transactionId);
        for (const auto& kv : params)
            body += wxString::Format("&%s=%s", kv.first, Escape(kv.second));

        std::string response;
        if (Perform(BaseUrl(action), &body, &response, err))
            return true;

        JsonParser parser;
        const json_value *value;
        return ParseEnvelope(response, &parser, &value, err);
    }

    bool GetBool(const wxString& action, bool *val, wxString *err)
    {
        JsonParser parser;
        const json_value *value;
        if (Get(action, &parser, &value, err))
            return true;
        *val = JsonAsBool(value);
        return false;
    }

    bool GetInt(const wxString& action, int *val, wxString *err)
    {
        JsonParser parser;
        const json_value *value;
        if (Get(action, &parser, &value, err))
            return true;
        *val = JsonAsInt(value);
        return false;
    }

    bool GetDouble(const wxString& action, double *val, wxString *err)
    {
        JsonParser parser;
        const json_value *value;
        if (Get(action, &parser, &value, err))
            return true;
        *val = JsonAsDouble(value);
        return false;
    }

    bool GetString(const wxString& action, wxString *val, wxString *err)
    {
        JsonParser parser;
        const json_value *value;
        if (Get(action, &parser, &value, err))
            return true;
        *val = JsonAsString(value);
        return false;
    }

    bool PutValue(const wxString& action, const wxString& key, const wxString& value, wxString *err)
    {
        return Put(action, { { key, value } }, err);
    }
    bool PutValue(const wxString& action, const wxString& key, bool value, wxString *err)
    {
        return PutValue(action, key, wxString(value ? "true" : "false"), err);
    }
    bool PutValue(const wxString& action, const wxString& key, double value, wxString *err)
    {
        return PutValue(action, key, wxString::Format("%.6f", value), err);
    }
    bool PutValue(const wxString& action, const wxString& key, int value, wxString *err)
    {
        return PutValue(action, key, wxString::Format("%d", value), err);
    }

    // Fetches the "imagearray" action as the binary ImageBytes format
    // (https://ascom-standards.org/newdocs/image-bytes.html) rather than the
    // classic JSON-array format. This is the modern, efficient transfer
    // format that Platform 6+ Alpaca camera drivers implement, and several
    // servers send it for ImageArray regardless of the client's Accept
    // header preference -- so this driver decodes it directly rather than
    // fighting content negotiation. *response holds the raw response body.
    bool GetImageBytes(std::string *response, wxString *err)
    {
        if (!m_curl)
        {
            *err = _("Alpaca: HTTP client not initialized");
            return true;
        }

        ++m_transactionId;
        wxString url =
            wxString::Format("%s?ClientID=%u&ClientTransactionID=%u", BaseUrl("imagearray"), m_clientId, m_transactionId);

        curl_slist *imageBytesHeaders = curl_slist_append(nullptr, "Accept: application/imagebytes");
        curl_easy_setopt(m_curl, CURLOPT_HTTPHEADER, imageBytesHeaders);

        bool failed = Perform(url, nullptr, response, err);

        curl_easy_setopt(m_curl, CURLOPT_HTTPHEADER, m_headers); // restore the default JSON header
        curl_slist_free_all(imageBytesHeaders);

        return failed;
    }
};

// ==========================================================================
// Discovery: UDP broadcast (Alpaca discovery protocol) + the management API
// ==========================================================================

enum
{
    ALPACA_DISCOVERY_PORT = 32227,
};

struct AlpacaServerInfo
{
    wxString host;
    int port;
};

struct AlpacaCameraDevice
{
    wxString host;
    int port;
    int deviceNumber;
    wxString name;
};

// One-shot GET against a host:port path that is NOT scoped under /api/v1/camera/{n}/
// (used only for the /management/... endpoints during discovery).
static bool FetchManagementJson(const wxString& host, int port, const wxString& path, JsonParser *parser,
                                const json_value **value, wxString *err)
{
    CURL *curl = curl_easy_init();
    if (!curl)
    {
        *err = _("Could not initialize HTTP client");
        return true;
    }

    wxString url = wxString::Format("http://%s:%d%s", host, port, path);
    std::string response;

    curl_slist *headers = curl_slist_append(nullptr, "Accept: application/json");

    curl_easy_setopt(curl, CURLOPT_URL, static_cast<const char *>(url.c_str()));
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, AlpacaWriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 2000L);
    curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    CURLcode res = curl_easy_perform(curl);
    curl_easy_cleanup(curl);
    curl_slist_free_all(headers);

    if (res != CURLE_OK)
    {
        *err = wxString::Format(_("HTTP error: %s"), curl_easy_strerror(res));
        return true;
    }

    if (!parser->Parse(response))
    {
        *err = _("invalid JSON response");
        return true;
    }

    *value = parser->Root();
    return false;
}

static std::vector<AlpacaCameraDevice> EnumAlpacaCamerasOnServer(const wxString& host, int port)
{
    std::vector<AlpacaCameraDevice> result;

    JsonParser parser;
    const json_value *root;
    wxString err;
    if (FetchManagementJson(host, port, "/management/v1/configureddevices", &parser, &root, &err))
    {
        Debug.Write(wxString::Format("Alpaca: failed to query configureddevices at %s:%d: %s\n", host, port, err));
        return result;
    }

    const json_value *devices = FindMember(root, "Value");
    json_for_each (dev, devices)
    {
        wxString devType = JsonAsString(FindMember(dev, "DeviceType"));
        if (!devType.IsSameAs("Camera", false))
            continue;

        AlpacaCameraDevice cam;
        cam.host = host;
        cam.port = port;
        cam.deviceNumber = JsonAsInt(FindMember(dev, "DeviceNumber"));
        cam.name = JsonAsString(FindMember(dev, "DeviceName"));
        result.push_back(cam);
    }

    return result;
}

// Broadcasts the Alpaca discovery datagram and collects replies for waitMs.
// Runs on the calling (GUI) thread and blocks for up to waitMs -- acceptable
// for a "Select Camera" button click, mirrors how INDI's device picker also
// blocks briefly while populating its device list.
static std::vector<AlpacaServerInfo> DiscoverAlpacaServers(int waitMs = 1500)
{
    std::vector<AlpacaServerInfo> found;

    wxIPV4address anyAddr;
    anyAddr.AnyAddress();
    wxDatagramSocket sock(anyAddr, wxSOCKET_NOWAIT);
    if (!sock.IsOk())
    {
        Debug.Write("Alpaca discovery: could not open UDP socket\n");
        return found;
    }

    int enable = 1;
    if (!sock.SetOption(SOL_SOCKET, SO_BROADCAST, &enable, sizeof(enable)))
        Debug.Write("Alpaca discovery: SetOption(SO_BROADCAST) failed -- broadcast send will likely be rejected by the OS\n");

    static const char discoveryMsg[] = "alpacadiscovery1";

    wxIPV4address broadcastAddr;
    broadcastAddr.Hostname("255.255.255.255");
    broadcastAddr.Service(ALPACA_DISCOVERY_PORT);
    sock.SendTo(broadcastAddr, discoveryMsg, sizeof(discoveryMsg) - 1);
    if (sock.Error())
        Debug.Write(wxString::Format("Alpaca discovery: broadcast SendTo failed, wxSocketError = %d\n", (int) sock.LastError()));

    // Broadcast to 255.255.255.255 is not always delivered back to a server
    // bound to the loopback interface (common when testing against a
    // simulator running on the same machine), so also probe localhost
    // directly with a unicast datagram.
    wxIPV4address localAddr;
    localAddr.Hostname("127.0.0.1");
    localAddr.Service(ALPACA_DISCOVERY_PORT);
    sock.SendTo(localAddr, discoveryMsg, sizeof(discoveryMsg) - 1);
    if (sock.Error())
        Debug.Write(wxString::Format("Alpaca discovery: localhost SendTo failed, wxSocketError = %d\n", (int) sock.LastError()));

    wxStopWatch sw;
    char buf[2048];
    while (sw.Time() < waitMs)
    {
        wxIPV4address fromAddr;
        sock.RecvFrom(fromAddr, buf, sizeof(buf) - 1);
        size_t n = sock.LastCount();
        if (n > 0 && n < sizeof(buf))
        {
            buf[n] = 0;
            JsonParser parser;
            if (parser.Parse(std::string(buf, n)))
            {
                const json_value *portVal = FindMember(parser.Root(), "AlpacaPort");
                if (portVal)
                {
                    AlpacaServerInfo info;
                    info.host = fromAddr.IPAddress();
                    info.port = JsonAsInt(portVal);

                    bool dup = false;
                    for (const auto& s : found)
                    {
                        if (s.host == info.host && s.port == info.port)
                        {
                            dup = true;
                            break;
                        }
                    }
                    if (!dup)
                    {
                        Debug.Write(wxString::Format("Alpaca discovery: found server at %s:%d\n", info.host, info.port));
                        found.push_back(info);
                    }
                }
            }
        }
        wxMilliSleep(50);
    }

    return found;
}

// ==========================================================================
// ImageBytes binary format decoding
// (https://ascom-standards.org/newdocs/image-bytes.html)
//
// Fixed 44-byte little-endian header followed by the raw pixel payload:
//   0  int32  MetadataVersion
//   4  int32  ErrorNumber
//   8  int32  ClientTransactionID
//   12 int32  ServerTransactionID
//   16 int32  DataStart      -- byte offset where pixel data begins
//   20 int32  ImageElementType
//   24 int32  TransmissionElementType -- actual on-wire element type
//   28 int32  Rank           -- 2 or 3
//   32 int32  Dimension1     -- X
//   36 int32  Dimension2     -- Y
//   40 int32  Dimension3     -- 0 for rank 2
// Pixel data is stored X-major/Y-minor (element [x,y] at index x*Dimension2+y),
// the same transposition as the JSON ImageArray format.
// ==========================================================================

enum AlpacaImageElementType
{
    ALPACA_IMG_UNKNOWN = 0,
    ALPACA_IMG_INT16 = 1,
    ALPACA_IMG_INT32 = 2,
    ALPACA_IMG_DOUBLE = 3,
    ALPACA_IMG_SINGLE = 4,
    ALPACA_IMG_UINT64 = 5,
    ALPACA_IMG_BYTE = 6,
    ALPACA_IMG_INT64 = 7,
    ALPACA_IMG_UINT16 = 8,
    ALPACA_IMG_UINT32 = 9,
};

static int32_t ReadLE32(const unsigned char *p)
{
    return (int32_t) ((uint32_t) p[0] | ((uint32_t) p[1] << 8) | ((uint32_t) p[2] << 16) | ((uint32_t) p[3] << 24));
}

static size_t AlpacaElementSize(int32_t elementType)
{
    switch (elementType)
    {
    case ALPACA_IMG_BYTE:
        return 1;
    case ALPACA_IMG_INT16:
    case ALPACA_IMG_UINT16:
        return 2;
    case ALPACA_IMG_INT32:
    case ALPACA_IMG_UINT32:
        return 4;
    default:
        return 0; // unsupported (Double/Single/Int64/UInt64)
    }
}

static long ReadAlpacaElement(const unsigned char *base, size_t index, int32_t elementType)
{
    switch (elementType)
    {
    case ALPACA_IMG_BYTE:
        return base[index];
    case ALPACA_IMG_INT16:
    {
        const unsigned char *p = base + index * 2;
        return (int16_t) (p[0] | (p[1] << 8));
    }
    case ALPACA_IMG_UINT16:
    {
        const unsigned char *p = base + index * 2;
        return (uint16_t) (p[0] | (p[1] << 8));
    }
    case ALPACA_IMG_INT32:
        return ReadLE32(base + index * 4);
    case ALPACA_IMG_UINT32:
        return (long) (uint32_t) ReadLE32(base + index * 4);
    default:
        return 0;
    }
}

// ==========================================================================
// CameraAlpaca
// ==========================================================================

class CameraAlpaca : public GuideCamera
{
    std::unique_ptr<AlpacaClient> m_client;
    wxString m_host;
    int m_port;
    int m_deviceNumber;
    wxSize m_maxSize;
    double m_driverPixelSize;
    bool m_canAbortExposure;
    bool m_canStopExposure;
    bool m_canSetCoolerTemperature;
    bool m_swapAxes;
    wxByte m_bitsPerPixel;
    wxByte m_curBin;
    wxRect m_roi;
    int m_gainMin;
    int m_gainMax;
    int m_curGain;

public:
    CameraAlpaca();
    ~CameraAlpaca();

    bool HasNonGuiCapture() override { return true; }
    wxByte BitsPerPixel() override { return m_bitsPerPixel; }
    bool CanSelectCamera() const override { return true; }
    bool EnumCameras(wxArrayString& names, wxArrayString& ids) override;
    bool Connect(const wxString& cameraId) override;
    bool Disconnect() override;
    bool Capture(usImage& img, const CaptureParams& captureParams) override;

    bool ST4HasGuideOutput() override;
    bool ST4HostConnected() override;
    bool ST4HasNonGuiMove() override { return true; }
    bool ST4PulseGuideScope(int direction, int duration) override;

    bool GetDevicePixelSize(double *devPixelSize) override;
    bool SetCoolerOn(bool on) override;
    bool SetCoolerSetpoint(double temperature) override;
    bool GetCoolerStatus(bool *on, double *setpoint, double *power, double *temperature) override;
    bool GetSensorTemperature(double *temperature) override;
    wxString GetSettingsSummary() override;
    int GetDefaultCameraGain() override;
    bool GetHardwareGain(int *gain) const override;

private:
    bool AbortExposure();
    bool PrepareImageBuffer(usImage& img, bool is_subframe, const wxRect& roi, long xsize, long ysize);
    bool DecodeImageBytes(const std::string& data, usImage& img, bool is_subframe, const wxRect& roi);
};

CameraAlpaca::CameraAlpaca()
{
    Connected = false;
    Name = _T("Alpaca Camera");
    m_hasGuideOutput = false;
    HasGainControl = false;
    HasSubframes = true;
    PropertyDialogType = PROPDLG_NONE;
    m_bitsPerPixel = 16;
    m_port = 0;
    m_deviceNumber = 0;
    m_canAbortExposure = false;
    m_canStopExposure = false;
    m_canSetCoolerTemperature = false;
    m_swapAxes = false;
    m_driverPixelSize = UnknownPixelSize;
    m_curBin = 1;
    m_gainMin = 0;
    m_gainMax = 0;
    m_curGain = INT_MIN;
}

CameraAlpaca::~CameraAlpaca() { }

bool CameraAlpaca::EnumCameras(wxArrayString& names, wxArrayString& ids)
{
    names.Clear();
    ids.Clear();

    Debug.Write("Alpaca: starting camera discovery\n");
    std::vector<AlpacaServerInfo> servers = DiscoverAlpacaServers();
    Debug.Write(wxString::Format("Alpaca: discovery found %d server(s)\n", (int) servers.size()));

    for (const auto& server : servers)
    {
        std::vector<AlpacaCameraDevice> cams = EnumAlpacaCamerasOnServer(server.host, server.port);
        for (const auto& cam : cams)
        {
            wxString name = cam.name.IsEmpty()
                ? wxString::Format(_T("Alpaca %s:%d #%d"), cam.host, cam.port, cam.deviceNumber)
                : wxString::Format(_T("%s (%s:%d)"), cam.name, cam.host, cam.port);
            wxString id = wxString::Format(_T("%s:%d:%d"), cam.host, cam.port, cam.deviceNumber);
            names.Add(name);
            ids.Add(id);
        }
    }

    return names.IsEmpty(); // true = error, per GuideCamera::EnumCameras contract
}

bool CameraAlpaca::Connect(const wxString& cameraId)
{
    wxString host = cameraId.BeforeFirst(':');
    wxString rest = cameraId.AfterFirst(':');
    long port = 0, devnum = 0;
    rest.BeforeFirst(':').ToLong(&port);
    rest.AfterFirst(':').ToLong(&devnum);

    if (host.IsEmpty())
    {
        return CamConnectFailed(
            _("No Alpaca camera selected. Use the \"Select Camera\" button to discover one on the network."));
    }

    m_host = host;
    m_port = (int) port;
    m_deviceNumber = (int) devnum;

    m_client.reset(new AlpacaClient(m_host, m_port, m_deviceNumber));
    if (!m_client->IsOk())
    {
        m_client.reset();
        return CamConnectFailed(_("Could not initialize the Alpaca HTTP client."));
    }

    wxString err;

    if (m_client->PutValue("connected", "Connected", true, &err))
    {
        m_client.reset();
        return CamConnectFailed(wxString::Format(_("Alpaca camera connect failed: %s"), err));
    }

    wxString name;
    if (!m_client->GetString("name", &name, &err) && !name.IsEmpty())
        Name = name;
    else
        Name = wxString::Format(_T("Alpaca Camera (%s:%d)"), m_host, m_port);

    bool canPulseGuide = false;
    if (m_client->GetBool("canpulseguide", &canPulseGuide, &err))
        Debug.AddLine("Alpaca camera: CanPulseGuide query failed: " + err);
    m_hasGuideOutput = canPulseGuide;

    if (m_client->GetBool("canabortexposure", &m_canAbortExposure, &err))
        m_canAbortExposure = false;
    if (m_client->GetBool("canstopexposure", &m_canStopExposure, &err))
        m_canStopExposure = false;

    bool hasShutter = false;
    if (!m_client->GetBool("hasshutter", &hasShutter, &err))
        HasShutter = hasShutter;

    int xsize, ysize;
    if (m_client->GetInt("cameraxsize", &xsize, &err))
    {
        m_client.reset();
        return CamConnectFailed(wxString::Format(_("Alpaca camera missing CameraXSize: %s"), err));
    }
    if (m_client->GetInt("cameraysize", &ysize, &err))
    {
        m_client.reset();
        return CamConnectFailed(wxString::Format(_("Alpaca camera missing CameraYSize: %s"), err));
    }
    m_maxSize.Set(xsize, ysize);
    m_swapAxes = false;

    double pixelSizeX = 0, pixelSizeY = 0;
    if (m_client->GetDouble("pixelsizex", &pixelSizeX, &err) || m_client->GetDouble("pixelsizey", &pixelSizeY, &err))
    {
        m_client.reset();
        return CamConnectFailed(wxString::Format(_("Alpaca camera missing pixel size: %s"), err));
    }
    m_driverPixelSize = wxMax(pixelSizeX, pixelSizeY);

    int maxBinX = 1, maxBinY = 1;
    m_client->GetInt("maxbinx", &maxBinX, &err);
    m_client->GetInt("maxbiny", &maxBinY, &err);
    MaxHwBinning = (wxByte) wxMax(1, wxMin(maxBinX, maxBinY));
    if (HwBinning > MaxHwBinning)
        HwBinning = MaxHwBinning;
    m_curBin = HwBinning;

    int sensorType = 0;
    if (!m_client->GetInt("sensortype", &sensorType, &err))
        HasBayer = sensorType > 1;

    int maxAdu = 0;
    if (!m_client->GetInt("maxadu", &maxAdu, &err))
        m_bitsPerPixel = maxAdu > 0 && maxAdu <= 255 ? 8 : 16;
    else
        m_bitsPerPixel = 16;

    // Cooler is optional; treat a failed query as "no cooler present" (same
    // graceful degradation as the classic ASCOM COM driver).
    bool coolerOn;
    HasCooler = !m_client->GetBool("cooleron", &coolerOn, &err);
    m_canSetCoolerTemperature = false;
    if (HasCooler)
    {
        bool canSetTemp = false;
        if (!m_client->GetBool("cansetccdtemperature", &canSetTemp, &err))
            m_canSetCoolerTemperature = canSetTemp;
    }

    // Gain is also optional. Unlike the classic ASCOM driver (which never
    // reads Gain/GainMin/GainMax), wire it up here when the device exposes a
    // numeric range. Devices that only expose the enumerated "Gains" string
    // list are left without gain control for now (documented future work).
    HasGainControl = false;
    int gainMin = 0, gainMax = 0;
    if (!m_client->GetInt("gainmin", &gainMin, &err) && !m_client->GetInt("gainmax", &gainMax, &err) && gainMax > gainMin)
    {
        m_gainMin = gainMin;
        m_gainMax = gainMax;
        HasGainControl = true;
    }
    m_curGain = INT_MIN;

    FrameSize = UNDEFINED_FRAME_SIZE;
    m_roi = wxRect();

    Connected = true;
    return false;
}

bool CameraAlpaca::Disconnect()
{
    if (!Connected)
    {
        Debug.Write("Alpaca camera: attempt to disconnect when not connected\n");
        return false;
    }

    bool failed = false;
    if (m_client)
    {
        wxString err;
        if (m_client->PutValue("connected", "Connected", false, &err))
        {
            Debug.AddLine("Alpaca camera disconnect error: " + err);
            pFrame->Alert(wxString::Format(_("Alpaca camera disconnect problem: %s"), err));
            failed = true;
        }
    }

    m_client.reset();
    Connected = false;
    return failed;
}

bool CameraAlpaca::GetDevicePixelSize(double *devPixelSize)
{
    if (!Connected)
        return true;
    *devPixelSize = m_driverPixelSize;
    return false;
}

bool CameraAlpaca::AbortExposure()
{
    if (!m_client || !(m_canAbortExposure || m_canStopExposure))
        return false;

    wxString err;
    bool failed = m_canAbortExposure ? m_client->Put("abortexposure", {}, &err) : m_client->Put("stopexposure", {}, &err);

    Debug.Write(wxString::Format("Alpaca AbortExposure returns err = %d\n", failed));
    return !failed;
}

// Allocates/clears img for an xsize x ysize frame (applying the same
// axis-swap heuristic cam_ascom.cpp uses for classic ASCOM cameras whose
// reported dimensions come back transposed vs CameraXSize/CameraYSize), or
// places a subframe within the already-known FrameSize. Shared by whichever
// ImageArray transfer format is in use.
bool CameraAlpaca::PrepareImageBuffer(usImage& img, bool is_subframe, const wxRect& roi, long xsize, long ysize)
{
    if (xsize <= 0 || ysize <= 0)
        return true;

    if (!is_subframe && !m_swapAxes && xsize < ysize && m_maxSize.x > m_maxSize.y)
    {
        Debug.Write(
            wxString::Format("Alpaca camera: array axes are flipped (%ldx%ld) vs (%dx%d)\n", xsize, ysize, m_maxSize.x, m_maxSize.y));
        m_swapAxes = true;
    }

    long width = m_swapAxes ? ysize : xsize;
    long height = m_swapAxes ? xsize : ysize;

    if (is_subframe)
    {
        if (FrameSize == UNDEFINED_FRAME_SIZE)
        {
            Debug.Write("internal error: taking subframe before full frame (Alpaca)\n");
            return true;
        }
        if (img.Init(FrameSize))
        {
            pFrame->Alert(_("Memory allocation error"));
            return true;
        }
        img.Clear();
        img.Subframe = roi;
    }
    else
    {
        FrameSize.Set((int) width, (int) height);
        if (img.Init(FrameSize))
        {
            pFrame->Alert(_("Memory allocation error"));
            return true;
        }
    }

    return false;
}

bool CameraAlpaca::DecodeImageBytes(const std::string& data, usImage& img, bool is_subframe, const wxRect& roi)
{
    static const size_t HEADER_SIZE = 44;
    if (data.size() < HEADER_SIZE)
    {
        Debug.Write(wxString::Format("Alpaca: ImageBytes response too short (%zu bytes)\n", data.size()));
        return true;
    }

    const unsigned char *buf = reinterpret_cast<const unsigned char *>(data.data());

    int32_t errorNumber = ReadLE32(buf + 4);
    int32_t dataStart = ReadLE32(buf + 16);
    int32_t transmissionType = ReadLE32(buf + 24);
    int32_t rank = ReadLE32(buf + 28);
    int32_t dim1 = ReadLE32(buf + 32);
    int32_t dim2 = ReadLE32(buf + 36);

    if (errorNumber != 0)
    {
        wxString msg;
        if ((size_t) dataStart > HEADER_SIZE && (size_t) dataStart <= data.size())
            msg = wxString::FromUTF8(data.data() + HEADER_SIZE, dataStart - HEADER_SIZE);
        Debug.Write(wxString::Format("Alpaca: ImageBytes error %d: %s\n", errorNumber, msg));
        return true;
    }

    if (rank < 2 || dim1 <= 0 || dim2 <= 0)
    {
        Debug.Write(wxString::Format("Alpaca: ImageBytes bad dimensions rank=%d dim1=%d dim2=%d\n", rank, dim1, dim2));
        return true;
    }

    size_t elemSize = AlpacaElementSize(transmissionType);
    if (elemSize == 0)
    {
        Debug.Write(wxString::Format("Alpaca: unsupported ImageBytes element type %d\n", transmissionType));
        return true;
    }

    if (dataStart < (int32_t) HEADER_SIZE)
        dataStart = (int32_t) HEADER_SIZE;

    long xsize = dim1;
    long ysize = dim2;

    size_t needed = (size_t) dataStart + (size_t) xsize * (size_t) ysize * elemSize;
    if (data.size() < needed)
    {
        Debug.Write(wxString::Format("Alpaca: ImageBytes payload truncated (have %zu, need %zu)\n", data.size(), needed));
        return true;
    }

    if (PrepareImageBuffer(img, is_subframe, roi, xsize, ysize))
        return true;

    long width = m_swapAxes ? ysize : xsize;
    long height = m_swapAxes ? xsize : ysize;

    const unsigned char *pixels = buf + dataStart;

    for (long x = 0; x < xsize; x++)
    {
        for (long y = 0; y < ysize; y++)
        {
            size_t index = (size_t) x * (size_t) ysize + (size_t) y;
            long value = ReadAlpacaElement(pixels, index, transmissionType);

            long px = m_swapAxes ? y : x;
            long py = m_swapAxes ? x : y;

            if (is_subframe)
            {
                if (px < roi.width && py < roi.height)
                    img.Pixel(roi.x + (int) px, roi.y + (int) py) = (unsigned short) wxClip(value, 0L, 65535L);
            }
            else if (px < width && py < height)
            {
                img.Pixel((int) px, (int) py) = (unsigned short) wxClip(value, 0L, 65535L);
            }
        }
    }

    return false;
}

bool CameraAlpaca::Capture(usImage& img, const CaptureParams& captureParams)
{
    if (!m_client)
        return true;

    int duration = captureParams.duration;
    int options = captureParams.captureOptions;

    bool takeSubframe = UseSubframes;
    wxRect roi(captureParams.subframe);

    if (roi.width <= 0 || roi.height <= 0)
        takeSubframe = false;

    bool binningChanged = false;
    if (HwBinning != m_curBin)
    {
        binningChanged = true;
        takeSubframe = false; // subframe may be out of bounds now
        if (HwBinning == 1)
            FrameSize.Set(m_maxSize.x, m_maxSize.y);
        else
            FrameSize = UNDEFINED_FRAME_SIZE; // unknown until we get a frame
    }

    if (takeSubframe && FrameSize == UNDEFINED_FRAME_SIZE)
        takeSubframe = false;

    if (!takeSubframe)
    {
        wxSize sz;
        if (FrameSize != UNDEFINED_FRAME_SIZE)
            sz = FrameSize;
        else
            sz.Set(m_maxSize.x / HwBinning, m_maxSize.y / HwBinning);
        roi = wxRect(sz);
    }

    wxString err;

    if (binningChanged)
    {
        if (m_client->PutValue("binx", "BinX", (int) HwBinning, &err) ||
            m_client->PutValue("biny", "BinY", (int) HwBinning, &err))
        {
            pFrame->Alert(wxString::Format(_("The Alpaca camera failed to set binning: %s"), err));
            return true;
        }
        m_curBin = HwBinning;
    }

    if (roi != m_roi)
    {
        if (m_client->PutValue("startx", "StartX", (int) roi.GetLeft(), &err) ||
            m_client->PutValue("starty", "StartY", (int) roi.GetTop(), &err) ||
            m_client->PutValue("numx", "NumX", (int) roi.GetWidth(), &err) ||
            m_client->PutValue("numy", "NumY", (int) roi.GetHeight(), &err))
        {
            pFrame->Alert(wxString::Format(_("The Alpaca camera failed to set the ROI: %s"), err));
            return true;
        }
        m_roi = roi;
    }

    if (HasGainControl)
    {
        int nativeGain = m_gainMin + (m_gainMax - m_gainMin) * wxClip(captureParams.gain, 0, 100) / 100;
        if (nativeGain != m_curGain)
        {
            if (m_client->PutValue("gain", "Gain", nativeGain, &err))
                Debug.AddLine("Alpaca camera: failed to set gain: " + err);
            else
                m_curGain = nativeGain;
        }
    }

    bool takeDark = HasShutter && ShutterClosed;

    std::vector<std::pair<wxString, wxString>> startParams = {
        { "Duration", wxString::Format("%.3f", (double) duration / 1000.0) },
        { "Light", takeDark ? "false" : "true" },
    };
    if (m_client->Put("startexposure", startParams, &err))
    {
        Debug.AddLine("Alpaca StartExposure failed: " + err);
        pFrame->Alert(wxString::Format(_("Alpaca error -- cannot start exposure: %s"), err));
        return true;
    }

    CameraWatchdog watchdog(duration, GetTimeoutMs());

    if (duration > 100)
    {
        if (WorkerThread::MilliSleep(duration - 100, WorkerThread::INT_ANY) &&
            (WorkerThread::TerminateRequested() || AbortExposure()))
        {
            return true;
        }
    }

    while (true) // wait for image to finish and download
    {
        wxMilliSleep(100);
        bool ready = false;
        if (m_client->GetBool("imageready", &ready, &err))
        {
            Debug.AddLine("Alpaca ImageReady poll failed: " + err);
            pFrame->Alert(wxString::Format(_("Exception thrown polling Alpaca camera: %s"), err));
            return true;
        }
        if (ready)
            break;
        if (WorkerThread::InterruptRequested() && (WorkerThread::TerminateRequested() || AbortExposure()))
            return true;
        if (watchdog.Expired())
        {
            DisconnectWithAlert(CAPT_FAIL_TIMEOUT);
            return true;
        }
    }

    std::string imageBytes;
    if (m_client->GetImageBytes(&imageBytes, &err))
    {
        Debug.AddLine("Alpaca ImageArray fetch failed: " + err);
        pFrame->Alert(wxString::Format(_("Error reading image: %s"), err));
        return true;
    }

    if (DecodeImageBytes(imageBytes, img, takeSubframe, roi))
    {
        pFrame->Alert(_("Error decoding Alpaca image data"));
        return true;
    }

    if (options & CAPTURE_SUBTRACT_DARK)
        SubtractDark(img);
    if ((options & CAPTURE_RECON) && HasBayer && captureParams.CombinedBinning() == 1)
        QuickLRecon(img);

    return false;
}

bool CameraAlpaca::ST4HasGuideOutput()
{
    return m_hasGuideOutput && Connected;
}

bool CameraAlpaca::ST4HostConnected()
{
    return Connected;
}

bool CameraAlpaca::ST4PulseGuideScope(int direction, int duration)
{
    if (!m_hasGuideOutput || !m_client)
        return true;

    if (!pMount || !pMount->IsConnected())
        return false;

    wxString err;
    std::vector<std::pair<wxString, wxString>> params = {
        { "Direction", wxString::Format("%d", direction) },
        { "Duration", wxString::Format("%d", duration) },
    };

    MountWatchdog watchdog(duration, 5000);

    if (m_client->Put("pulseguide", params, &err))
    {
        Debug.AddLine("Alpaca PulseGuide failed: " + err);
        return true;
    }

    if (watchdog.Time() < duration) // likely returned right away, not after the move -- poll
    {
        while (true)
        {
            bool moving = false;
            if (m_client->GetBool("ispulseguiding", &moving, &err))
            {
                Debug.AddLine("Alpaca IsPulseGuiding poll failed: " + err);
                return true;
            }
            if (!moving)
                break;
            wxMilliSleep(50);
            if (WorkerThread::TerminateRequested())
                return true;
            if (watchdog.Expired())
            {
                Debug.Write("Alpaca: watchdog timed out waiting for IsPulseGuiding to clear\n");
                return true;
            }
        }
    }

    return false;
}

bool CameraAlpaca::SetCoolerOn(bool on)
{
    if (!HasCooler || !m_client)
    {
        Debug.Write("Alpaca camera: has no cooler!\n");
        return true;
    }

    if (!Connected)
        return true;

    wxString err;
    if (m_client->PutValue("cooleron", "CoolerOn", on, &err))
    {
        Debug.AddLine(wxString::Format("Alpaca error turning camera cooler %s: %s", on ? "on" : "off", err));
        pFrame->Alert(wxString::Format(_("Alpaca error turning camera cooler %s: %s"), on ? _("on") : _("off"), err));
        return true;
    }

    return false;
}

bool CameraAlpaca::SetCoolerSetpoint(double temperature)
{
    if (!HasCooler || !m_canSetCoolerTemperature || !m_client)
    {
        Debug.Write("Alpaca camera: cannot set cooler temperature\n");
        return true;
    }

    if (!Connected)
        return true;

    wxString err;
    if (m_client->PutValue("setccdtemperature", "SetCCDTemperature", temperature, &err))
    {
        Debug.AddLine("Alpaca error setting cooler setpoint: " + err);
        return true;
    }

    return false;
}

bool CameraAlpaca::GetCoolerStatus(bool *on, double *setpoint, double *power, double *temperature)
{
    if (!HasCooler || !m_client)
        return true;

    wxString err;

    if (m_client->GetBool("cooleron", on, &err))
    {
        Debug.AddLine("Alpaca error getting CoolerOn property: " + err);
        return true;
    }

    if (m_client->GetDouble("ccdtemperature", temperature, &err))
    {
        Debug.AddLine("Alpaca error getting CCDTemperature property: " + err);
        return true;
    }

    if (m_canSetCoolerTemperature)
    {
        if (m_client->GetDouble("setccdtemperature", setpoint, &err))
        {
            Debug.AddLine("Alpaca error getting SetCCDTemperature property: " + err);
            return true;
        }
    }
    else
        *setpoint = *temperature;

    if (m_client->GetDouble("coolerpower", power, &err))
        *power = 100.0;

    return false;
}

bool CameraAlpaca::GetSensorTemperature(double *temperature)
{
    if (!m_client)
        return true;

    wxString err;
    if (m_client->GetDouble("ccdtemperature", temperature, &err))
    {
        Debug.AddLine("Alpaca error getting CCDTemperature property: " + err);
        return true;
    }

    return false;
}

wxString CameraAlpaca::GetSettingsSummary()
{
    wxString binningStr = GetBinning() > 1 ? wxString::Format(_T(", Binning %dx%d"), GetBinning(), GetBinning()) : wxString();
    wxString gainStr = HasGainControl ? wxString::Format(_T(", Gain %d%%"), GuideCameraGain) : wxString();

    return wxString::Format(_T("Camera = Alpaca %s:%d #%d%s%s\n"), m_host, m_port, m_deviceNumber, binningStr, gainStr);
}

int CameraAlpaca::GetDefaultCameraGain()
{
    return 0;
}

bool CameraAlpaca::GetHardwareGain(int *gain) const
{
    if (!HasGainControl || !m_client)
        return false;

    wxString err;
    int nativeGain;
    if (m_client->GetInt("gain", &nativeGain, &err))
        return false;

    *gain = nativeGain;
    return true;
}

GuideCamera *ASCOMAlpacaCameraFactory::MakeAlpacaCamera()
{
    return new CameraAlpaca();
}

#endif // ALPACA_CAMERA
