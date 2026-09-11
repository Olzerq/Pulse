package main

import (
	"os"

	"github.com/Olzerq/Pulse/internal/api"
	"github.com/Olzerq/Pulse/internal/app"
)

func main() {
	os.Exit(app.Run("api", api.Run))
}
