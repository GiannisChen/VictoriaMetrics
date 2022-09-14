package testutils

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

func ReadAllFloat64File(file string) ([]float64, error) {
	f, err := os.Open(file)
	defer f.Close()
	if err != nil {
		return nil, err
	}

	dst := make([]float64, 0)
	r := bufio.NewReader(f)
	for true {
		var num float64
		if n, err := fmt.Fscanf(r, "%f\n", &num); n == 0 || (err != nil && err != io.EOF) {
			if err == io.EOF {
				return dst, nil
			}
			return dst, nil
		} else {
			dst = append(dst, num)
			if err == io.EOF {
				return dst, nil
			}
		}
	}
	return dst, nil
}

func ReadFileName(file string, length uint) ([]string, error) {
	f, err := os.Open(file)
	defer f.Close()
	if err != nil {
		return nil, err
	}

	dst := make([]string, length)
	r := bufio.NewReader(f)
	for i := uint(0); i < length; i++ {
		var name string
		if n, err := fmt.Fscanf(r, "%s\n", &name); n == 0 || (err != nil && err != io.EOF) {
			return nil, err
		} else {
			dst[i] = name
		}
	}
	return dst, nil
}
