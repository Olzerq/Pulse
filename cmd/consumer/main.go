package main

import (
	"os"

	"pulse/internal/app"
	"pulse/internal/consumer"
)

func main() {
	os.Exit(app.Run("consumer", consumer.Run))
}
