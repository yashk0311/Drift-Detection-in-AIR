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
 * EventCollectorCD.cpp
 *
 *  Created on: Dec 26, 2018
 *      Author: vinu.venugopal
 */

#include <unistd.h>
#include "../serialization/Serialization.hpp"
#include "EventCollector_cd.hpp"
#include <bits/stdc++.h>
#include <chrono>
#include <fstream>
#include <istream>
using namespace std;

EventCollectorCD::EventCollectorCD(int tag, int rank, int worldSize) : Vertex(tag, rank, worldSize),
																	   lat("/home/yash/Desktop/Clg/7th_Sem/SDS/Project/src/lat.csv"),
																	   tp("/home/yash/Desktop/Clg/7th_Sem/SDS/Project/src/throughputs.csv")
{

	// Global stats
	sum_latency = 0;
	sum_counts = 0;
	num_messages = 0;
	test_bag = "";

	S_CHECK(if (rank == 0) {
		datafile.open("Data/results" + to_string(rank) + ".tsv");
	})

	D(cout << "EventCollectorCD [" << tag << "] CREATED @ " << rank << endl;)
}

EventCollectorCD::~EventCollectorCD()
{
	D(cout << "EventCollectorCD [" << tag << "] DELETED @ " << rank << endl;)
}

void EventCollectorCD::batchProcess()
{
	D(cout << "EventCollectorCD->BATCHPROCESS [" << tag << "] @ " << rank << endl;)
}

long int EventCollectorCD::getCurrentTimestampMillisec()
{
	using namespace std::chrono;

	auto currentTimePoint = system_clock::now();
	auto duration = duration_cast<milliseconds>(currentTimePoint.time_since_epoch());

	return duration.count();
}
void EventCollectorCD::streamProcess(int channel)
{
	D(cout << "EventCollectorCD->STREAMPROCESS [" << tag << "] @ " << rank
		   << " IN-CHANNEL " << channel << endl;)

	if (rank == 0)
	{
		Message *inMessage;
		list<Message *> *tmpMessages = new list<Message *>();
		Serialization sede;

		EventCD eventPCReg;

		int c = 0;
		long int WID_no = 0;
		while (ALIVE)
		{

			pthread_mutex_lock(&listenerMutexes[channel]);

			while (inMessages[channel].empty())
			{
				pthread_cond_wait(&listenerCondVars[channel],
								  &listenerMutexes[channel]);
			}

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

				sede.unwrap(inMessage);

				int offset = sizeof(int) + (inMessage->wrapper_length * sizeof(WrapperUnit));

				int event_count = (inMessage->size - offset) / sizeof(EventCD);
				// cout << event_count << endl;

				int i = 0, j = 0;
				while (i < event_count)
				{
					// cout<<i<<endl;
					sede.YSBdeserializeCD(inMessage, &eventPCReg,
										  offset + (i * sizeof(EventCD)));
					// sum_latency += eventPCReg.latency;
					// cout<<eventPCReg.bag<<endl;
					cout << "bag contents: " << eventPCReg.bag << endl;
					lat << WID_no << ", " << eventPCReg.bag << endl;
					WID_no++;
					test_bag += eventPCReg.bag;
					// count += eventPCReg.count;
					S_CHECK(
						datafile
							<< eventPCReg.WID << "\t"
							<< eventPCReg.count << " \t"
							<< eventPCReg.latency
							<< endl;

					)
					i++;
				}
				// sum_counts += event_count; // count of distinct c_id's processed
				num_messages++;

				// tp << WID_no << ", " << eventPCReg.count << endl;

				// cout << "The WID is: " <<eventPCReg.WID << endl;
				// cout << "Latency of this window: " << eventPCReg.latency << endl;
				// cout << "The number of events in that window id is: " << count << endl;
				// cout << "Total latency uptil this window is: " << sum_latency << endl;
				// cout<<"test bag contents: "<<test_bag<<endl;
				cout << "------------------------------------------------------" << endl;
				delete inMessage; // delete message from incoming queue
				c++;
				
			}

			tmpMessages->clear();
		}

		delete tmpMessages;
	}
}
