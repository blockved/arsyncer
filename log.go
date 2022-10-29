package arsyncer

import (
	"github.com/inconshreveable/log15"
)

func NewLog(serverName string, logPath string) log15.Logger {
	lg := log15.New("module", serverName)

	// default logger handle
	//h := lg.GetHandler()
	lg.SetHandler(
		log15.MultiHandler(
			log15.CallerFileHandler(log15.LvlFilterHandler(log15.LvlDebug, log15.Must.FileHandler(logPath, log15.JsonFormat()))),
		))

	return lg
}
