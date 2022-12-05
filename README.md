# door2door

[![Build Status](https://app.travis-ci.com/gianmarcodonetti/door2door.svg?branch=main)](https://app.travis-ci.com/github/gianmarcodonetti/door2door/builds)
[![codecov](https://codecov.io/gh/gianmarcodonetti/door2door/branch/main/graph/badge.svg?token=PF72U1MVOP)](https://codecov.io/gh/gianmarcodonetti/door2door)


## Introduction
**door2door** collects the live position of all vehicles in its fleet in real-time via a GPS sensor in each
vehicle. These vehicles run in operating periods that are managed by **door2door**’s operators. An API is
responsible for collecting information from the vehicles and place it on an S3 bucket, in raw format, to
be consumed.

The goal of this challenge is to automate the build of a simple yet scalable data lake and data warehouse
that will enable our BI team to answer questions like:

_What is the average distance traveled by our vehicles during an operating period?_

We would like to ask you to develop a solution that:
1. Fetches the data from the bucket on a daily basis and stores it on a data lake;
2. Processes and extracts the main events that occurred during operating periods;
3. Store the transformed data on a data warehouse. The data warehouse should be SQL-queriable
(SQL database or using something like AWS Athena).