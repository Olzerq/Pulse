package main

import (
	"os"

	"pulse/internal/api"
	"pulse/internal/app"
)

func main() {
	os.Exit(app.Run("api", api.Run))
}
