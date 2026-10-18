package objectstore

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestObjectStore(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ObjectStore Suite")
}
