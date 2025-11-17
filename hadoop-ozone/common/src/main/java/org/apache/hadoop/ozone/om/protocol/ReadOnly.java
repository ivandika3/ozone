package org.apache.hadoop.ozone.om.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker interface used to annotate methods that are readonly.
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@InterfaceAudience.Private
@InterfaceStability.Evolving
public @interface ReadOnly {
  /**
   * @return if true, when processing the rpc call of the target method, the
   * server side will wait if server state id is behind client (msync). If
   * false, the method will be processed regardless of server side state.
   */
  boolean isCoordinated() default false;
}
