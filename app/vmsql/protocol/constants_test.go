/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package protocol

import (
	"testing"
)

func TestConstants(t *testing.T) {
	var i byte
	for i = 0; i < COM_RESET_CONNECTION+2; i++ {
		CommandString(i)
	}
}
