package main

import "sync/atomic"

func filter(ss []string, test func(string) bool) (ret []string) {
    for _, s := range ss {
        if test(s) {
            ret = append(ret, s)
        }
    }
    return
}

func mergeAtomic(cs ...<-chan ObjectInfo) <-chan ObjectInfo {
	out := make(chan ObjectInfo)
	var i int32
	atomic.StoreInt32(&i, int32(len(cs)))
	for _, c := range cs {
		go func(c <-chan ObjectInfo) {
			for v := range c {
				out <- v
			}
			if atomic.AddInt32(&i, -1) == 0 {
				close(out)
			}
		}(c)
	}
	return out
}