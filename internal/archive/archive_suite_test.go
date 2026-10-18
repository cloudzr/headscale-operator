package archive

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestArchive(t *testing.T) {
	RegisterFailHandler(Fail)
	// Keep the settle re-read, without waiting seconds for every claim.
	ClaimSettleDelay = time.Millisecond
	RunSpecs(t, "Archive Suite")
}
