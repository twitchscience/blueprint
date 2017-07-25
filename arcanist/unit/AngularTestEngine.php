<?php

/**
 * Runs angular unit tests by executing run_angular_tests.sh, which is expected to be located at
 * the project root.
 *
 * For more details about custom class definition, check out the docs:
 * https://secure.phabricator.com/book/phabricator/article/arcanist_lint_unit/
 */
final class AngularTestEngine extends ArcanistUnitTestEngine {
  public function run() {
    $future = new ExecFuture('./run_angular_tests.sh');
    do {
      list($stdout, $stderr) = $future->read();
      echo $stdout;
      echo $stderr;
      sleep(0.5);
    } while (!$future->isReady());
    list($error, $stdout, $stderr) = $future->resolve();

    // ArcanistUnitTestResult class definition:
    // https://github.com/phacility/arcanist/blob/master/src/unit/ArcanistUnitTestResult.php
    $result = new ArcanistUnitTestResult();
    $result->setName('Angular tests');
    $result->setResult($error != 0 ? ArcanistUnitTestResult::RESULT_FAIL : ArcanistUnitTestResult::RESULT_PASS);
    return array($result);
  }
}
