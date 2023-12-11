
/**
 * Copyright (c) 2020 University of Luxembourg. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are
 * permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this list of
 * conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list
 * of conditions and the following disclaimer in the documentation and/or other materials
 * provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of its contributors may be
 * used to endorse or promote products derived from this software without specific prior
 * written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE UNIVERSITY OF LUXEMBOURG AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PUCDOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * THE UNIVERSITY OF LUXEMBOURG OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
 * OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
 * EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 **/

/*
 * CUSUM_cd.cpp
 *
 *  Created on:
 *      Author: vinu.venugopal
 */

#include "CUSUM_cd.hpp"

#include <mpi.h>
// #include <__threading_support>
#include <cstring>
#include <iostream>
#include <iterator>
#include <list>
#include <string>
#include <vector>
#include <regex>
#include <bits/stdc++.h>

#include "../communication/Message.hpp"
#include "../serialization/Serialization.hpp"

using namespace std;

CUSUM_cd::CUSUM_cd(int tag, int rank, int worldSize, string pattern) : Vertex(tag, rank, worldSize)
{
    this->pattern = pattern;
    D(cout << "CUSUM_cd [" << tag << "] CREATED @ " << rank << endl;)
    THROUGHPUT_LOG(
        datafile.open("Data/tp_log" + to_string(rank) + ".tsv");)
}

CUSUM_cd::~CUSUM_cd()
{
    D(cout << "CUSUM_cd [" << tag << "] DELETED @ " << rank << endl;)
}

void CUSUM_cd::batchProcess()
{
    cout << "CUSUM_cd->BATCHPROCESS [" << tag << "] @ " << rank << endl;
}

double calculateDistance(const vector<string> &dataPoint1, const vector<string> &dataPoint2)
{
    int differences = 0;

    for (int i = 0; i < dataPoint1.size(); i++)
    {
        if (dataPoint1[i] != dataPoint2[i])
        {
            differences++;
        }
    }

    return (double)differences / dataPoint1.size();
}

double calculateStandardDeviation(const vector<double> &distances)
{
    double mean = 0.0;
    for (const double &distance : distances)
    {
        mean += distance;
    }
    mean /= distances.size();

    double variance = 0.0;
    for (const double &distance : distances)
    {
        variance += pow(distance - mean, 2);
    }
    variance /= distances.size() - 1;

    return sqrt(variance);
}

double calculateEuclideanDistance(const vector<int> &dataPoint1, const vector<double> &dataPoint2)
{
    double distance = 0.0;
    for (int i = 0; i < dataPoint1.size(); ++i)
    {
        distance += pow(dataPoint1[i] - dataPoint2[i], 2);
    }
    return sqrt(distance);
}

double calculateMovingAverage(const deque<double> &window)
{
    double sum = accumulate(window.begin(), window.end(), 0.0);
    return sum / window.size();
}

double calculateMedianAbsoluteDeviation(const deque<double> &window, double median)
{
    deque<double> deviations;
    for (double value : window)
    {
        deviations.push_back(abs(value - median));
    }
    sort(deviations.begin(), deviations.end());
    return deviations[deviations.size() / 2];
}

void CUSUM_cd::streamProcess(int channel)
{

    D(cout << "CUSUM_cd->STREAMPROCESS [" << tag << "] @ " << rank
           << " IN-CHANNEL " << channel << endl;)

    Message *inMessage, *outMessage;
    list<Message *> *tmpMessages = new list<Message *>();
    Serialization sede;

    // WrapperUnit wrapper_unit;
    EventCD eventCD;
    EventAdwin eventAdwin;

    // eventFT.max_event_time = LONG_MAX;

    int c = 0;

    while (ALIVE)
    {

        pthread_mutex_lock(&listenerMutexes[channel]);

        while (inMessages[channel].empty())
            pthread_cond_wait(&listenerCondVars[channel],
                              &listenerMutexes[channel]);

        while (!inMessages[channel].empty())
        {
            inMessage = inMessages[channel].front();
            inMessages[channel].pop_front();
            tmpMessages->push_back(inMessage);
        }

        pthread_mutex_unlock(&listenerMutexes[channel]);

        const double K = 1.5;
        const int WINDOW_SIZE = 5;

        while (!tmpMessages->empty())
        {

            inMessage = tmpMessages->front();
            tmpMessages->pop_front();

            D(cout << "CUSUM_cd->POP MESSAGE: TAG [" << tag << "] @ " << rank
                   << " CHANNEL " << channel << " BUFFER " << inMessage->size
                   << endl;)

            sede.unwrap(inMessage);

            int offset = sizeof(int) + (inMessage->wrapper_length * sizeof(WrapperUnit));

            outMessage = new Message(inMessage->size - offset,
                                     inMessage->wrapper_length); // create new message with max. required capacity

            memcpy(outMessage->buffer, inMessage->buffer, offset); // simply copy header from old message for now!
            outMessage->size += offset;

            int event_count = (inMessage->size - offset) / sizeof(EventCD);

            D(cout << "THROUGHPUT: " << event_count << " @RANK-" << rank << " TIME: " << (long int)MPI_Wtime() << endl;)

            THROUGHPUT_LOG(
                datafile << event_count << "\t"
                         << rank << "\t"
                         << (long int)MPI_Wtime()
                         << endl;)

            int i = 0, j = 0;

            double sumUp = 0.0;
            double sumDown = 0.0;
            int lineNumber = 1;
            deque<double> window;

            while (i < event_count)
            {

                sede.YSBdeserializeCD(inMessage, &eventCD,
                                      offset + (i * sizeof(EventCD)));

                eventAdwin.event_time = eventCD.event_time;

                string line = eventCD.bag;
                istringstream iss(line);
                vector<int> dataPoint;
                int item;
                while (iss >> item)
                {
                    dataPoint.push_back(item);
                }

                double distance = calculateEuclideanDistance(dataPoint, {0.0});
                window.push_back(distance);

                if (window.size() > WINDOW_SIZE)
                {
                    window.pop_front();
                }
                double expectedDistance = calculateMovingAverage(window);

                double median = window[window.size() / 2];
                double residual = calculateMedianAbsoluteDeviation(window, median);

                sumUp = max(0.0, sumUp + residual - K);
                sumDown = min(0.0, sumDown + residual + K);

                if (sumUp > 0.0)
                {
                    // cout << "Concept drift detected at line number " << lineNumber << "! Upward CUSUM: " << sumUp << endl;
                    sumUp = 0;
                    sumDown = 0;
                    string ans = "1";
                    memcpy(eventAdwin.drift, ans.c_str(), strlen(ans.c_str()) + 1);
                }
                else if (sumDown < 0.0)
                {
                    // cout << "Concept drift detected at line number " << lineNumber << "! Downward CUSUM: " << sumDown << endl;
                    sumDown = 0;
                    sumUp = 0;
                    string ans = "1";
                    memcpy(eventAdwin.drift, ans.c_str(), strlen(ans.c_str()) + 1);
                }
                else{
                    string ans = "0";
                    memcpy(eventAdwin.drift, ans.c_str(), strlen(ans.c_str()) + 1); 
                }

                lineNumber++;

                sede.YSBserializeAdwin(&eventAdwin, outMessage);

                sede.YSBdeserializeAdwin(outMessage, &eventAdwin,
                                         outMessage->size - sizeof(EventAdwin));
                // sede.YSBprintAdwin(&eventAdwin);

                j++;

                i++;
            }

            // Replicate data to all subsequent vertices, do not actually reshard the data here
            int n = 0;
            for (vector<Vertex *>::iterator v = next.begin(); v != next.end();
                 ++v)
            {
                int wid = eventCD.event_time / AGG_WIND_SPAN;
                int idx = n * worldSize + 0; // always keep workload on same rank
                // int idx =  wid % worldSize + 0; // always keep workload on same rank

                if (PIPELINE)
                {

                    // Pipeline mode: immediately copy message into next operator's queue
                    pthread_mutex_lock(&(*v)->listenerMutexes[idx]);
                    (*v)->inMessages[idx].push_back(outMessage);

                    D(cout << "CUSUM_cd->PIPELINE MESSAGE [" << tag << "] #"
                           << c << " @ " << rank << " IN-CHANNEL " << channel
                           << " OUT-CHANNEL " << idx << " SIZE "
                           << outMessage->size << " CAP "
                           << outMessage->capacity << endl;)

                    pthread_cond_signal(&(*v)->listenerCondVars[idx]);
                    pthread_mutex_unlock(&(*v)->listenerMutexes[idx]);
                }
                else
                {

                    // Normal mode: synchronize on outgoing message channel & send message

                    // cout<<"Window id: "<<wid<<" Time stamp: "<<eventRG.event_time<<endl;
                    pthread_mutex_lock(&senderMutexes[idx]);
                    outMessages[idx].push_back(outMessage);

                    D(cout << "CUSUM_cd->PUSHBACK MESSAGE [" << tag << "] #"
                           << c << " @ " << rank << " IN-CHANNEL " << channel
                           << " OUT-CHANNEL " << idx << " SIZE "
                           << outMessage->size << " CAP "
                           << outMessage->capacity << endl;)

                    pthread_cond_signal(&senderCondVars[idx]);
                    pthread_mutex_unlock(&senderMutexes[idx]);
                }

                n++;
                break; // only one successor node allowed!
            }

            delete inMessage;
            c++;
        }

        tmpMessages->clear();
    }

    delete tmpMessages;
}
