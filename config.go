package gocelery

type Option func(*Config)

func WithIgnoreResult() Option {
	return func(c *Config) {
		c.ignoreResult = true
	}
}

type Config struct {
	ignoreResult bool
}
