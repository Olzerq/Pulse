package main

import (
	"os"

	"pulse/internal/app"
	"pulse/internal/pinger"
)

func main() {
	os.Exit(app.Run("pinger", pinger.Run))
}
