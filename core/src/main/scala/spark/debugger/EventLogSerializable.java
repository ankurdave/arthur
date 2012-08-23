package spark.debugger;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for transient fields that should be serialized to the event log.
 *
 * This class is in Java instead of Scala because Scala does not support runtime-visible annotations
 * (see SI-32), and event log serialization must look for @EventLogSerializable fields at runtime.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface EventLogSerializable {}
