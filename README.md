Here are my solutions for counting unique IP in a huge file.
I have zero experience with GO, I dont know best practices, patterns, etc, so I wrote those solutions using my previous Python and Java experience.
Overall, I enjoyed using GO and I definitely will be learning it more.

I have written some commentaries in the code, but the code and ideas are pretty easy to follow anyway.

Also, I havent tested the solutions on Windows or MacOS. There were no restrictions for the task, and I am using Ubuntu.
I think that these solutions might not work on Windows or MacOS, due to different ways of reading/writing to a file. But I think that this problem should not be that hard to fix. 

I have made 3 solutions for this problem, written in Python and Go

Solution1.go and Solution2.go are based on counting using bitmap.

Solution 1 is faster but costs around 512MB of RAM. Runtime for 8 billion IPs from the provided file was around 1.5 minutes. The amount of counted unique IP was 1 billion. 
Solution 2 is slower but costs 512MB of disk space instead.

Solution3.go is my implementation of Hyper Log Log algorithm.
It will estimate the amount of unique IP based on the probability. 
It will cost almost no RAM and will be the fastest solution out of all. The estimation is pretty close when dealing with relatively small unique counts.

There are some places for improvement in these solutions. 

For example, do dynamic memory allocation. There are 2^32 different IP, but depending on the situation, it is not that likely to get all 2^32 IP.
