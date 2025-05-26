package diskmanager

type Table interface {
	Insert(key int32, val string) error
	Select(key int32) (string, error)
	Delete(key int32) error
	Update(key int32, val string) error
	SelectAll() error
}

// Compulsary initdb before initTable else it might cause some bugs
func InitTable(d *DiskManager) Table {
	if d.IsTree {
		d.Cursor = d.SrtOff
		return tree{
			table: d,
		}
	}
	return d
}
