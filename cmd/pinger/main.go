package main

import (
	"os"

	"github.com/Olzerq/Pulse/internal/app"
	"github.com/Olzerq/Pulse/internal/pinger"
)

func main() {
	os.Exit(app.Run("pinger", pinger.Run))
}
