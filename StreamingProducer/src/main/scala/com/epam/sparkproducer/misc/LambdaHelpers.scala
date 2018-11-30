package com.epam.sparkproducer.misc

import java.util.concurrent.Callable
import java.util.function.{Consumer, Supplier, Function}

/**
 * Class for converting scala functions to functional interfaces in java.
 */
object LambdaHelpers {

  /**
   * Coverts function to a callable.
   *
   * @param fun the function.
   * @tparam T type parameter.
   * @return a callable object.
   */
  def funToCallable[T](fun: () => T): Callable[T] = new Callable[T] {
    def call() = fun()
  }

  /**
   * Coverts function to consumer.
   *
   * @param fun the function.
   * @tparam T type parameter.
   * @return a consumer.
   */
  def funToConsumer[T](fun: T => Unit): Consumer[T] = new Consumer[T] {
    def accept(v: T): Unit = fun(v)
  }

  /**
   * Converts function to supplier.
   *
   * @param fun the function
   * @tparam T type parameter.
   * @return a supplier.
   */
  def funToSupplier[T](fun: () => T): Supplier[T] = new Supplier[T] {
    override def get() = fun()
  }

  /**
   * Converts function to java.util.function.Function.
   *
   * @param fun the function.
   * @tparam InT  input type parameter.
   * @tparam OutT output type parameter.
   * @return a java.util.function.Function.
   */
  def funToFunction[InT, OutT](fun: InT => OutT): Function[InT, OutT] = new Function[InT, OutT] {
    override def apply(t: InT): OutT = fun(t)
  }
}
