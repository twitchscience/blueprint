# logger
logger is a library for writing structured json logs to stderr.

It is a wrapper around [logrus](https://github.com/Sirupsen/logrus) that
automatically formats as JSON, provides some default context
(env, pid, host, and caller), and can capture output written to the default logger
(since external dependencies will not be using this logger).

## Usage
To use logger, just import this library, then call `logger.Init(<level>)` from
your main or init.
Then, you can log output using functions like `logger.Infoln("message")` and
`logger.Fatal("ohno")`. See the
[logrus](https://github.com/Sirupsen/logrus) documentation for more functions you can
use.

## Capturing default logger output
To capture output from the default logger, use `logger.CaptureDefault()`.
To create a golang logger that will have its output forwarded to logger, use
`logger.GetCapturedLogger()`.
If you do these, don't provide additional configuration to the captured logger,
or you may break the capturing.
