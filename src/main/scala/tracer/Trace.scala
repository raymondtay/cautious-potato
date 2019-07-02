package thalesdigital.io.tracer

import io.opentracing.Span
import io.opentracing.Scope
import io.opentracing.Tracer
import io.opentracing.util.GlobalTracer
import io.jaegertracing.Configuration
import io.jaegertracing.Configuration.ReporterConfiguration
import io.jaegertracing.Configuration.SamplerConfiguration
import io.jaegertracing.internal.JaegerTracer

import org.apache.spark.sql._

import cats._, data._, implicits._

import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.metrics.Metric

/**
 * Extend from this trait if you wish to have a global tracer acquired for
 * your application
 * @author Raymond
 * @version 1.0
 */
trait GloblTracer {

  // Note: You need to escape the name if there are spaces.
  val defaultTracerName : String

  // We attempt to initialized the tracer from the environment and if it
  // succeeds, we will retrieve it otherwise we will get the no-op tracer.
  implicit lazy val defaultTrace : Tracer = initTracer(defaultTracerName) match { case Right(t) => t; case Left(error) => GlobalTracer.get }

  def startSpan(label: String) = Applicative[Id].lift((t: Tracer) => t.buildSpan(label).start)

  def startScope(label: String) = Applicative[Id].lift((t: Tracer) => t.buildSpan(label).startActive(true))

  def closeSpan(span: Span) = Reader{ (df: DataFrame) =>
    span.finish
    df
  }

  def traceRun[A, B, C](f: (A, B, Span) => C)(a: A)(b: B)(label: String)(implicit tracer: Tracer) = {
    val scope : Scope = startScope(label)(tracer)
    val span = scope.span
    val childSpan = tracer.buildSpan("Running effect").asChildOf(span).start
    val outcome = Apply[Id].map3(a, b, childSpan)(f)
    childSpan.finish
    span.finish
    outcome
  }

  def traceRun2[A, B, C, D]( f: (A, B, Span, (DataFrame => C)) => D)(a: A)(b: B)(g: DataFrame => C)(label: String)(implicit tracer: Tracer) = {
    val scope = startScope(label)(tracer)
    val span = scope.span
    val childSpan = tracer.buildSpan("Running effect").asChildOf(span).start
    val outcome = Apply[Id].map4(a, b, childSpan, g)(f)
    childSpan.finish
    span.finish
    outcome
  }

  /**
   * Sends the log to the Jaeger implementation
   * @param event key name to be displayed for this span
   * @param eventInfo description of the event
   * @param span the current active span
   * @return
   */
  def logEvent(event: String, eventInfo: String) : Reader[Span, Unit] =
    Reader { (span: Span) => 
      import scala.collection.JavaConverters._
      Applicative[Id].map(span)( (s:Span) => s.log(Map(event -> eventInfo).asJava))
    }


  /**
   * Sends the log to the Jaeger implementation along with sending back the
   * outcome
   * @param event key name to be displayed for this span
   * @param eventInfo description of the event
   * @param span the current active span
   * @param outcome something to carry along
   * @return
   */
  def logEventWithOutcome[A](event: String, eventInfo: String, span: Span) =
    Reader { (outcome: A) => 
      logEvent(event, eventInfo).run(span) *> outcome
    }

  /**
   * Sends the metric information contained in the [[AnalyzerContext]]
   * to the opentracing logger seated in the environment.
   *
   * @param event key name to be displayed for this span
   * @param eventInfo description of the event
   * @param span the current active span
   * @param outcome something to carry along
   * @return
   */
  def logMetrics(label: String)(implicit tracer: Tracer) =
    Reader{ (ctx: AnalyzerContext) =>
      val span = startSpan(label)(tracer)
      val childSpan = tracer.buildSpan("Send metrics").asChildOf(span).start
      val id = java.util.UUID.randomUUID
      logEvent(s"event-${id}", s"About to send ${ctx.allMetrics.size} metrics to logger...")(childSpan)

      // the metrics are a lazy stream, so reify it
      ctx.allMetrics.toList.zipWithIndex.map((p: (Metric[_], Int)) => logEvent(s"metric-${p._2}", p._1.toString)(childSpan))

      logEvent(s"event-${id}", "All metrics sent.")(childSpan)
      childSpan.finish
      span.finish
    }

  /**
   * The initialization of the Tracer object in open-tracing. There are
   * actually a lot more options available to the developer.
   * @param serviceName name of the service
   * @return a Left(error) indicating there's some kind of error or
   * Right(tracer) which holds the actual tracer.
   */
  def initTracer : Reader[ServiceName, Either[_, Tracer]] =
    Reader{ (serviceName: String) =>
      Either.catchNonFatal {
        val samplerConfig = SamplerConfiguration.fromEnv.withType("const").withParam(1)
        val reporterConfig = ReporterConfiguration.fromEnv.withLogSpans(true)

        val config = new Configuration(serviceName).withSampler(samplerConfig).withReporter(reporterConfig)
        config.getTracer()
      }
    }

}

