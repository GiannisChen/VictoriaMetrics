package sqlerror

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSqlError(t *testing.T) {
	{
		sqlerr := NewSQLError(1, "i.am.error.man")
		assert.Equal(t, "i.am.error.man (errno 1105) (sqlstate HY000)", sqlerr.Error())
	}

	{
		sqlerr := NewSQLErrorf(1, "i.am.error.man%s", "xx")
		assert.Equal(t, "i.am.error.manxx (errno 1105) (sqlstate HY000)", sqlerr.Error())
	}

	{
		sqlerr := NewSQLError(ER_NO_DB_ERROR)
		assert.Equal(t, "No database selected (errno 1046) (sqlstate 3D000)", sqlerr.Error())
	}
}
