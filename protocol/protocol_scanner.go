package protocol

import (
	"onionscanv3/config"
	"onionscanv3/report"
)

type Scanner interface {
	ScanProtocol(hiddenService string, onionscanConfig *config.OnionScanConfig, report *report.OnionScanReport)
}
