package spark.api.java.function;

import scala.reflect.ClassManifest;
import scala.reflect.ClassManifest$;
import scala.runtime.AbstractFunction2;

import java.io.Serializable;

public abstract class Function2<T1, T2, R> extends WrappedFunction2<T1, T2, R>
  implements Serializable {

  public abstract R call(T1 t1, T2 t2) throws Exception;

  public ClassManifest<R> returnType() {
    return (ClassManifest<R>) ClassManifest$.MODULE$.fromClass(Object.class);
  }
}

