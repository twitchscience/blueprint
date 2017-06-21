package transformer

var (
	// ValidTransforms lists the types that a column in an event is allowed to have.
	ValidTransforms = []string{
		"bigint",
		"bool",
		"float",
		"int",
		"ipAsn",
		"ipAsnInteger",
		"ipCity",
		"ipCountry",
		"ipRegion",
		"varchar",
		"f@timestamp@unix",
		"f@timestamp@unix-utc",
		"userIDWithMapping",
	}
)
