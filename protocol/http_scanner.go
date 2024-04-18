package protocol

import (
	"fmt"
	"onionscanv3/config"
	"onionscanv3/report"

	"net/http"
	"onionscanv3/spider"
	"onionscanv3/utils"
)

type HTTPProtocolScanner struct {
	Client *http.Client
}

func (hps *HTTPProtocolScanner) ScanProtocol(hiddenService string, osc *config.OnionScanConfig, report *report.OnionScanReport) {
	//检查连接
	// HTTP
	osc.LogInfo(fmt.Sprintf("Checking %s http(80)\n", hiddenService))
	conn, err := utils.GetNetworkConnection(hiddenService, 80, osc.TorProxyAddress, osc.Timeout)
	if conn != nil {
		conn.Close()
	}
	//有服务则爬取
	if err != nil {
		osc.LogInfo("Failed to connect to service on port 80\n")
		report.WebDetected = false
	} else {
		osc.LogInfo("Found potential service on http(80)\n")
		report.WebDetected = true
		wps := new(spider.OnionSpider)
		wps.Crawl(report.HiddenService, osc, report)
	}
}
