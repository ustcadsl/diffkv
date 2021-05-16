//
//  timer.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_TIMER_H_
#define YCSB_C_TIMER_H_

#include <sys/time.h>

namespace utils {
class Timer{
private:
    timeval start_;
    timeval end_;

public:
    void Start(){
      gettimeofday(&start_, nullptr);
    }

    //return us elapsed
    double End(){
      gettimeofday(&end_, nullptr);
      return (end_.tv_sec-start_.tv_sec)*1000000 + (end_.tv_usec-start_.tv_usec);
    }
};


} // utils

#endif // YCSB_C_TIMER_H_

