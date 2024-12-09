Here is my solutions for counting unique IP in a huge file.

I have made 3 solutions for this problem, written in Python and Go

Solution1.go and Solution2.go are based on counting using bitmap.

Solution 1 is faster, but costs around 512MB of RAM.
Solution 2 is slower, but costs 512MB of disk space instead.

Solution3.go is my implementation of Hyper Log Log algorithm.
It will estimate the amount of unique IP based on the probability. 
It will cost almost no RAM and will be the fastest solution out of all. The estimation is pretty close when dealing with relatively small unique counts.

There are some places for improvement for these solutions. For example, do dynamic memory allocation. There are 2^32 different IP, but depending on the situation, it is not that likely to get all 2^32 IP.
