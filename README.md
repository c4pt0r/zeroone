Zero-One
=======

一个分布式，面向服务的消息分发系统, 基于zmq

用于将各个服务分离，客户端仍可以透明的进行调用


    $ python ./zo_broker.py 5555
    $ python ./test_echo_worker.py
    $ python ./test_client.py


##Architecture
![](http://rfc.zeromq.org/local--files/spec:7/figure1.png)
