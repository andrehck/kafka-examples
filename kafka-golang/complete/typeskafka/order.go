package typeskafka

type Order struct {
	Id    int     `json:"id"`
	Name  string  `json:"name"`
	Valor float64 `json:"valor"`
	Email string  `json:"email"`
}
