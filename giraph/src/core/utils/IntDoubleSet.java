package core.utils;

/**
 * A set of two integers and a double.
 */
public class IntDoubleSet {
  /** First element. */
  private int first;
  /** Second element. */
  private int second;
  /**Third element. */
  private double third;

  /** Constructor.
   *
   * @param fst First element
   * @param snd Second element
   * @param thd Third element
   */
  public IntDoubleSet(int fst, int snd, double thd) {
    first = fst;
    second = snd;
    third = thd;
  }

  /**
   * Get the first element.
   *
   * @return The first element
   */
  public int getFirst() {
    return first;
  }

  /**
   * Set the first element.
   *
   * @param first The first element
   */
  public void setFirst(int first) {
    this.first = first;
  }

  /**
   * Get the second element.
   *
   * @return The second element
   */
  public int getSecond() {
    return second;
  }

  /**
   * Set the second element.
   *
   * @param second The second element
   */
  public void setSecond(int second) {
    this.second = second;
  }

    /**
   * Get the third element.
   *
   * @return The second element
   */
  public double getThird() {
    return third;
  }

  /**
   * Set the third element.
   *
   * @param third The third element
   */
  public void setThird(double third) {
    this.third = third;
  }
}