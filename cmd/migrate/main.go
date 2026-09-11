package main

import (
	"os"

	"github.com/Olzerq/Pulse/internal/app"
	"github.com/Olzerq/Pulse/internal/migrate"
)

func main() {
	os.Exit(app.Run("migrate", migrate.Run))
}
