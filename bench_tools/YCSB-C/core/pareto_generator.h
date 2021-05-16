//
// Created by wujy on 4/10/20.
//

#ifndef YCSB_C_PARETOGENERATOR_H
#define YCSB_C_PARETOGENERATOR_H

#include "generator.h"
#include <random>
#include "iostream"

namespace ycsbc {

class Random64 {
private:
  std::mt19937_64 generator_;

public:
  explicit Random64(uint64_t s) : generator_(s) {}

  // Generates the next random number
  uint64_t Next() { return generator_(); }

  // Returns a uniformly distributed value in the range [0..n-1]
  // REQUIRES: n > 0
  uint64_t Uniform(uint64_t n) {
    return std::uniform_int_distribution<uint64_t>(0, n - 1)(generator_);
  }

  // Randomly returns true ~"1/n" of the time, and false otherwise.
  // REQUIRES: n > 0
  bool OneIn(uint64_t n) { return Uniform(n) == 0; }

  // Skewed: pick "base" uniformly from range [0,max_log] and then
  // return "base" random bits.  The effect is to pick a number in the
  // range [0,2^max_log-1] with exponential bias towards smaller numbers.
  uint64_t Skewed(int max_log) {
    return Uniform(uint64_t(1) << Uniform(max_log + 1));
  }
};

class ParetoGenerator : public Generator<uint64_t> {
public:
  ParetoGenerator(uint64_t num, double theta, double k, double sigma)
      : num_(num), theta_(theta), k_(k), sigma_(sigma), rand_(1000) {
    Next();
  }
  uint64_t Next() {
    int64_t rand_v = rand_.Next()%num_;
    double u = static_cast<double>(rand_v) / num_;
    double ret;
    if (k_ == 0.0) {
      ret = theta_ - sigma_ * std::log(u);
    } else {
      ret = theta_ + sigma_ * (std::pow(u, -1 * k_) - 1) / k_;
    }
    last_value_ = Value(static_cast<int64_t>(ceil(ret)));
    return last_value_;
  }

  uint64_t Last() { return last_value_; }

private:
  uint64_t num_;
  double theta_;
  double k_;
  double sigma_;
  double last_value_;
  uint64_t max_value_{128 * 1024};
  Random64 rand_;

  uint64_t Value(uint64_t v) {
  return (v < 0 || v > max_value_)?Next():v;
    // return v < 0 ? 10 : (v > max_value_ ? v % max_value_ : v);
  }
};

} // namespace ycsbc

#endif // YCSB_C_PARETOGENERATOR_H