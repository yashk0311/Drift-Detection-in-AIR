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
 * ADWIN_cd.cpp
 *
 *  Created on:
 *      Author: vinu.venugopal
 */

#include "ADWIN_cd.hpp"

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

ADWIN_cd::ADWIN_cd(int tag, int rank, int worldSize, string pattern) : Vertex(tag, rank, worldSize)
{
	this->pattern = pattern;
	D(cout << "ADWIN_cd [" << tag << "] CREATED @ " << rank << endl;)
	THROUGHPUT_LOG(
		datafile.open("Data/tp_log" + to_string(rank) + ".tsv");)
}

ADWIN_cd::~ADWIN_cd()
{
	D(cout << "ADWIN_cd [" << tag << "] DELETED @ " << rank << endl;)
}

void ADWIN_cd::batchProcess()
{
	cout << "ADWIN_cd->BATCHPROCESS [" << tag << "] @ " << rank << endl;
}

struct Bucket
{
	int size;
	double sum;
};

vector<Bucket> updateBuckets(vector<Bucket> buckets, double mean, int bucketSize)
{
	if (buckets.size() > bucketSize)
	{
		buckets.erase(buckets.begin());
	}

	buckets.push_back({1, mean});
	return buckets;
}

pair<double, int> calculateMean(vector<Bucket> buckets)
{
	double sum = 0.0;
	int count = 0;

	for (const auto &bucket : buckets)
	{
		sum += bucket.sum;
		count += bucket.size;
	}

	return make_pair(sum / count, count);
}

double calculateVariance(vector<Bucket> buckets, double mean)
{
	double variance = 0.0;
	int count = 0;

	for (const auto &bucket : buckets)
	{
		variance += bucket.size * pow(bucket.sum / bucket.size - mean, 2);
		count += bucket.size;
	}

	return variance / count;
}

pair<bool, vector<Bucket>> detectDrift(double variance, int totalSize, int bucketSize, double lastChange, double delta)
{
	if (totalSize <= bucketSize)
	{
		return make_pair(false, vector<Bucket>());
	}

	double p = 1.0 / totalSize;
	double q = 1.0 / (totalSize - bucketSize);
	double z = sqrt(2 * log(1 / delta) * p * q);

	double driftThreshold = z * sqrt(variance / bucketSize) + 2 * log(1 / delta) / bucketSize;

	if (variance - lastChange > driftThreshold)
	{
		lastChange = variance;
		vector<Bucket> buckets;
		buckets.push_back({0, 0.0});
		totalSize = 0;
		lastChange = 0.0;
		return make_pair(true, buckets);
	}

	return make_pair(false, vector<Bucket>());
}

void ADWIN_cd::streamProcess(int channel)
{

	D(cout << "ADWIN_cd->STREAMPROCESS [" << tag << "] @ " << rank
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

		while (!tmpMessages->empty())
		{

			inMessage = tmpMessages->front();
			tmpMessages->pop_front();

			D(cout << "ADWIN_cd->POP MESSAGE: TAG [" << tag << "] @ " << rank
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

			string line;
			int line_c = 0;
			vector<Bucket> buckets;
			buckets.push_back({0, 0.0});
			int totalSize = 0;
			double lastChange = 0.0;
			int bucketSize = 10;
			double delta = 0.03;
			//*/
			while (i < event_count)
			{

				sede.YSBdeserializeCD(inMessage, &eventCD,
									  offset + (i * sizeof(EventCD)));

				eventAdwin.event_time = eventCD.event_time;
				
				line = eventCD.bag;
				istringstream iss(line);
				string number;
				vector<double> values;
				iss >> number;
				while (iss >> number)
				{
					values.push_back(stod(number));
				}

				double mean = accumulate(values.begin(), values.end(), 0.0) / values.size();
				buckets = updateBuckets(buckets, mean, bucketSize);

				pair<double, int> meanAndCount = calculateMean(buckets);
				double globalMean = meanAndCount.first;
				double variance = calculateVariance(buckets, globalMean);

				pair<bool, vector<Bucket>> driftAndBuckets = detectDrift(variance, totalSize, bucketSize, lastChange, delta);
				if (driftAndBuckets.first)
				{
					string ans = "1";
					memcpy(eventAdwin.drift, ans.c_str(), strlen(ans.c_str()) + 1); // Use strlen for correct memory copy
					buckets = driftAndBuckets.second;
				}
				else
				{
					string ans = "0";
					memcpy(eventAdwin.drift, ans.c_str(), strlen(ans.c_str()) + 1); // Use strlen for correct memory copy
				}
				// cout << "drift value in filter: " << eventAdwin.drift << endl;

				totalSize++;
				line_c++;
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

					D(cout << "ADWIN_cd->PIPELINE MESSAGE [" << tag << "] #"
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

					D(cout << "ADWIN_cd->PUSHBACK MESSAGE [" << tag << "] #"
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
