package encoding

import (
	"fmt"
	"github.com/cdipaolo/goml/base"
	"github.com/cdipaolo/goml/linear"
	"testing"
)

func TestSACML(t *testing.T) {
	trainFourDX, trainFourDY, err := base.LoadDataFromCSV("/home/giannischen/go/src/giannischen@nuaa.edu.cn/VictoriaMetrics/lib/encoding/data/train.csv")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(len(trainFourDX), len(trainFourDY))

	model := linear.NewSoftmax(base.StochasticGA, 1e-5, 0, 5, 70000, trainFourDX, trainFourDY)
	err = model.Learn()

	var guess []float64
	var trueCount, falseCount int64
	testFourDX, testFourDY, err := base.LoadDataFromCSV("/home/giannischen/go/src/giannischen@nuaa.edu.cn/VictoriaMetrics/lib/encoding/data/test.csv")
	if err != nil {
		t.Fatal(err)
	}
	for i, dx := range testFourDX {
		guess, err = model.Predict(dx)
		if err != nil {
			t.Fatal(err)
		}
		if guess[0] == testFourDY[i] {
			trueCount++
		} else {
			falseCount++
		}
		fmt.Println(guess[0])
	}
	fmt.Println(trueCount, falseCount, float64(trueCount)/float64(trueCount+falseCount))
}
