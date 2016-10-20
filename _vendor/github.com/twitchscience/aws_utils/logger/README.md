# logger
logger is a library for writing structured json logs to stderr.

It is a wrapper around [logrus](https://github.com/Sirupsen/logrus) that
automatically formats as JSON and provides some default context
(env, pid, host, and caller). It  can capture output written to the default logger
(since external dependencies will not be using this logger), and can add a rollbar 
rollbar [hook](https://github.com/Sirupsen/logrus#hooks) to send all 
errors/panics/fatals to rollbar.

## Usage
To use logger, just import this library, then call `logger.Init(<level>)`
 from your main or init.
Then, you can log output using functions like `logger.Infoln("message")` and
`logger.Fatal("ohno")`. See the
[logrus](https://github.com/Sirupsen/logrus) documentation for more functions you can
use.

To use the rollbar logger, initialize with:
```
logger.InitWithRollbar(<level>, <token>, <env>)
```
Add this to main to send all top level panics to rollbar:
```
defer logger.LogPanic()
```

To capture and log panics in other goroutines, spawn them with
```
logger.Go( <func> )
```
When using logger.Go, be careful about variables in loops. Unlike normal
goroutines, you can't pass arguments (including objects with methods called on them).
As an example, to convert:
```
for i, o := range objects {
    go o.worker(i)
}
```
you will need to do one of the two following:
```
// Turn the loop variables into local function variables.
for i, o := range objects {
    logger.Go(func(o Obj, i int) func() {
        return func() { o.worker(i) }
    }(o, i))
}
```
or
```
// Alias the loop variables in the block scope.
for i, o := range objects {
    i := i
    o := o
    logger.Go(func() { o.worker(i) })
}
```
If you don't do one of the above, `i` and `o` will be shared across all goroutine invocations,
which means that they will probably be the value of the last iteration of the loop.

And in your shutdown code, include `logger.Wait()` to wait for any remaining
logs to be sent to rollbar before exiting.

## Capturing default logger output
To capture output from the default logger, use `logger.CaptureDefault()`.
To create a golang logger that will have its output forwarded to logger, use
`logger.GetCapturedLogger()`.
If you do these, don't provide additional configuration to the captured logger,
or you may break the capturing.
