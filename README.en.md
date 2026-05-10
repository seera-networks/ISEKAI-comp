<h1 align="center">ISEKAI Computation<img src="ISEKAI_arch.png" alt="ISEKAI Computation Architecture"/></h1>

- [Overview](#overview)
- [QuickStart](#quickstart)
- [ISEKAI Data Server](#isekai-data-server)
- [ISEKAI Data Provider Module](#isekai-data-provider-module)
- [ISEKAI Computation Module](#isekai-computation-module)


# Overview
- This repository provides companion software for using the secure computation platform ISEKAI Computation, offered by SEERA Networks Corporation.

# QuickStart
- If you are trying it for the first time, please read [QuickStart](QuickStart.md).

# ISEKAI Data Server
- [ISEKAI Data Server](isekai-data-server) is a server that provides data to ISEKAI Computation.
It can also run on a client machine behind NAT.

# ISEKAI Data Provider Module
- [ISEKAI Data Provider Module](mods/mod-http-data-flight) runs on ISEKAI Computation and retrieves data by connecting to the ISEKAI Data Server.

# ISEKAI Computation Module
- The ISEKAI Computation Module runs on ISEKAI Computation and performs various processing tasks such as calculating averages.
- It is not publicly available at this time.
