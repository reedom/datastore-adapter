package datastoreadapter

type Config struct {
	// Datastore kind name.
	// Optional. (Default: "casbin")
	Kind string
	// Datastore namespace.
	// Optional. (Default: "")
	Namespace string
}

