package engine.util

import org.slf4j.LoggerFactory

/**
  * And generic logger to be used across the Engine framework, it uses sl4j with logback as a backend
  */
trait EngineLogger {
  protected val logger = LoggerFactory.getLogger(getClass)
}
