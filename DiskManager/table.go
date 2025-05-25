package diskmanager

type Table interface {
	Insert(key int32, val string) error
	Select(key int32) (string, error)
	Delete(key int32) error
	Update(key int32, val string) error
	SelectAll() error
}
