package goes_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestGoes(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Goes Suite")
}
