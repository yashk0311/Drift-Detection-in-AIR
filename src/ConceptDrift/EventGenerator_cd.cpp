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
 * EventGenerator.cpp
 *
 *  Created on: Dec 06, 2018
 *      Author: vinu.venugopal
 */

#include <mpi.h>
#include <unistd.h>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <iterator>
#include <list>
#include <string>
#include <vector>
#include <chrono>
#include <bits/stdc++.h>
#include "../communication/Window.hpp"
#include "EventGenerator_cd.hpp"

using namespace std;

EventGeneratorCD::EventGeneratorCD(int tag, int rank, int worldSize,
								   unsigned long tp, unsigned long dr) : Vertex(tag, rank, worldSize)
{
	this->throughput = tp;
	this->drift_rate = dr;
	cout << "AIR INSTANCE AT RANK " << (rank + 1) << "/" << worldSize << " | TP: " << throughput << " | MSG/SEC/RANK: " << PER_SEC_MSG_COUNT << " | AGGR_WINDOW: " << AGG_WIND_SPAN << "ms" << endl;

	S_CHECK(
		datafile.open("Data/data" + to_string(rank) + ".tsv");)

	D(cout << "EventGeneratorCD [" << tag << "] CREATED @ " << rank << endl;)
}

EventGeneratorCD::~EventGeneratorCD()
{
	D(cout << "EventGeneratorCD [" << tag << "] DELTETED @ " << rank << endl;)
}

void EventGeneratorCD::batchProcess()
{
	D(
		cout << "EventGeneratorCD->BATCHPROCESS: TAG [" << tag << "] @ "
			 << rank << endl;)
}

void EventGeneratorCD::streamProcess(int channel)
{

	D(
		cout << "EventGeneratorCD->STREAMPROCESS: TAG [" << tag << "] @ "
			 << rank << " CHANNEL " << channel << endl;)

	Message *message;
	Message **outMessagesPerSec = new Message *[PER_SEC_MSG_COUNT];

	WrapperUnit wrapper_unit;
	EventCD eventCD;

	int wrappers_per_msg = 1; // currently only one wrapper per message!
	int events_per_msg = this->throughput / PER_SEC_MSG_COUNT / worldSize;

	cout << "Events per message: " << events_per_msg << endl;

	long int start_time = (long int)MPI_Wtime();
	long int t1, t2;

	int iteration_count = 0, c = 0;

	while (ALIVE)
	{

		t1 = MPI_Wtime();

		int msg_count = 0;
		while (msg_count < PER_SEC_MSG_COUNT)
		{

			outMessagesPerSec[msg_count] = new Message(
				events_per_msg * sizeof(EventCD), wrappers_per_msg);

			// Message header
			long int time_now = (start_time + iteration_count) * 1000;
			wrapper_unit.window_start_time = time_now + 999; // this is the max-event-end-time
			wrapper_unit.completeness_tag_numerator = 1;
			wrapper_unit.completeness_tag_denominator = PER_SEC_MSG_COUNT * worldSize * AGG_WIND_SPAN / 1000;

			memcpy(outMessagesPerSec[msg_count]->buffer, &wrappers_per_msg,
				   sizeof(int));
			memcpy(outMessagesPerSec[msg_count]->buffer + sizeof(int),
				   &wrapper_unit, sizeof(WrapperUnit));
			// Declare the eventCD variable
			EventCD eventCD;

			outMessagesPerSec[msg_count]->size += sizeof(int) + outMessagesPerSec[msg_count]->wrapper_length * sizeof(WrapperUnit);

			// Message body

			getNextMessage(&eventCD, &wrapper_unit,
						   outMessagesPerSec[msg_count], events_per_msg, time_now, throughput, drift_rate);

			// Debug output ---
			Serialization sede;

			// for (int e = 0; e < events_per_msg; e++)
			// {
			// sede.YSBdeserializeDG(outMessagesPerSec[msg_count], &eventRG,
			// 						  sizeof(int) + (outMessagesPerSec[msg_count]->wrapper_length * sizeof(WrapperUnit)) + (e * sizeof(EventDG)));
			// }

			msg_count++;
			c++;
		}

		t2 = MPI_Wtime();
		while ((t2 - t1) < 1)
		{
			usleep(100);
			t2 = MPI_Wtime();
		}

		msg_count = 0;
		while (msg_count < PER_SEC_MSG_COUNT)
		{
			// Replicate data to all subsequent vertices, do not actually reshard the data here
			int n = 0;
			for (vector<Vertex *>::iterator v = next.begin(); v != next.end();
				 ++v)
			{

				int idx = n * worldSize + rank; // always keep workload on same rank

				if (PIPELINE)
				{

					// Pipeline mode: immediately copy message into next operator's queue
					pthread_mutex_lock(&(*v)->listenerMutexes[idx]);
					(*v)->inMessages[idx].push_back(
						outMessagesPerSec[msg_count]);

					D(
						cout << "EventGeneratorCD->PIPELINE MESSAGE [" << tag
							 << "] #" << c << " @ " << rank
							 << " IN-CHANNEL " << channel
							 << " OUT-CHANNEL " << idx << " SIZE "
							 << outMessagesPerSec[msg_count]->size << " CAP "
							 << outMessagesPerSec[msg_count]->capacity << endl;)

					pthread_cond_signal(&(*v)->listenerCondVars[idx]);
					pthread_mutex_unlock(&(*v)->listenerMutexes[idx]);
				}
				else
				{

					// Normal mode: synchronize on outgoing message channel & send message
					pthread_mutex_lock(&senderMutexes[idx]);
					outMessages[idx].push_back(outMessagesPerSec[msg_count]);

					D(
						cout << "EventGeneratorCD->PUSHBACK MESSAGE [" << tag
							 << "] #" << c << " @ " << rank
							 << " IN-CHANNEL " << channel
							 << " OUT-CHANNEL " << idx << " SIZE "
							 << outMessagesPerSec[msg_count]->size << " CAP "
							 << outMessagesPerSec[msg_count]->capacity << endl;)

					pthread_cond_signal(&senderCondVars[idx]);
					pthread_mutex_unlock(&senderMutexes[idx]);
				}

				n++;
				break; // only one successor node allowed!
			}

			msg_count++;
		}

		iteration_count++;
	}
}
vector<string> EventGeneratorCD::random_sample(vector<string> &items, int num)
{
	random_shuffle(items.begin(), items.end());
	vector<string> result(items.begin(), items.begin() + num);
	return result;
}

string EventGeneratorCD::generate_bag(unsigned long tp, unsigned long dr)
{

	vector<string> items = {"item1", "item2", "item3", "item4", "item5", "item6", "item7", "item8", "item9", "item10"};
	int throughput = tp;
	int drift_rate = dr;
	int start = MPI_Wtime();
	vector<string> trending_items = random_sample(items, 5);
	vector<string> remaining_items;
	vector<string> replaced_items;
	for (auto &item : items)
	{
		if (find(trending_items.begin(), trending_items.end(), item) == trending_items.end())
		{
			remaining_items.push_back(item);
		}
	}
	int bag_count = 0;
	int change_count = 0;
	int change_rate = drift_rate / 2;
	auto pause_start = chrono::system_clock::now();
	string ans = "";
	bool pause = false;

	while (true)
	{
		// Generate a bag of trending items
		if(MPI_Wtime()-start > 1 || bag_count > throughput)
			break;

		vector<string> bag = random_sample(trending_items, 5);
		sort(bag.begin(), bag.end());
		// cout << "Bag " << bag_count + 1 << ": ";
		for (auto &item : bag)
		{
			ans += item;
			ans += " ";
		}

		bag_count += 1;
		this_thread::sleep_for(chrono::seconds(1) / throughput); // Wait for the next bag

		if (pause && chrono::duration_cast<chrono::seconds>(chrono::system_clock::now() - pause_start).count() <= 3)
		{
			// cout << "Pause is " << pause << ", time difference is " << chrono::duration_cast<chrono::seconds>(chrono::system_clock::now() - pause_start).count() << endl;
			continue;
		}
		else if (pause && chrono::duration_cast<chrono::seconds>(chrono::system_clock::now() - pause_start).count() > 3)
		{
			remaining_items.clear();
			for (auto &item : items)
			{
				if (find(trending_items.begin(), trending_items.end(), item) == trending_items.end())
				{
					remaining_items.push_back(item);
				}
			}
			pause = false;
			// cout<<"pause is "<<pause<<", time difference is "<<chrono::duration_cast<chrono::seconds>(chrono::system_clock::now() - pause_start).count()<<endl;
		}
		else if (bag_count >= drift_rate)
		{
			// After drift_rate bags, gradually change the trending items
			if (bag_count % change_rate == 0 && !remaining_items.empty())
			{
				vector<string> items_to_replace;
				for (const auto &item : trending_items)
				{
					if (find(replaced_items.begin(), replaced_items.end(), item) == replaced_items.end())
					{
						items_to_replace.push_back(item);
					}
				}

				if (!items_to_replace.empty())
				{
					string item_to_replace = items_to_replace[rand() % items_to_replace.size()];
					trending_items.erase(remove(trending_items.begin(), trending_items.end(), item_to_replace), trending_items.end());

					string new_item = remaining_items[rand() % remaining_items.size()];
					trending_items.push_back(new_item);
					replaced_items.push_back(new_item);
					remaining_items.erase(remove(remaining_items.begin(), remaining_items.end(), new_item), remaining_items.end());

					change_count += 1;
				}
			}
			// cout << "Change count: " << change_count << endl;
			if (change_count == 5)
			{
				// After all trending items have been changed, start the pause
				pause_start = chrono::system_clock::now();
				pause = true;
				change_count = 0;
			}
		}
	}
	// cout<<"inside generate_bag function ans is: "<<ans<<endl;
	return ans;
}

void EventGeneratorCD::getNextMessage(EventCD *event, WrapperUnit *wrapper_unit,
									  Message *message, int events_per_msg, long int time_now, unsigned long tp, unsigned long dr)
{

	Serialization sede;
	long int max_time = 0;
	// Serializing the events
	int i = 0;
	// cout << events_per_msg << endl;

	while (i < events_per_msg)
	{
		// cout << "hi" << endl;
		string seq = generate_bag(tp, dr);

		memcpy(event->bag, seq.c_str(), 30);
		event->event_time = (time_now);

		// S_CHECK(
		// 	datafile << event->event_time << "\t"
		// 			 // divide this by the agg wid size
		// 			 << event->event_time / AGG_WIND_SPAN << "\t"
		// 			 // divide this by the agg wid size
		// 			 << rank << "\t" << i << "\t" << event->event_type << "\t"
		// 			 << event->ad_id << endl;);

		sede.YSBserializeCD(event, message);
		cout << "event_time: " << event->event_time << "\tbag contents: " << event->bag << endl;
		cout<<endl;
		if (max_time < event->event_time)
			max_time = event->event_time;

		i++;
	}
	wrapper_unit->window_start_time = max_time;
}

int EventGeneratorCD::myrandom(int min, int max)
{ // range : [min, max)
	static bool first = true;
	if (first)
	{
		srand(time(NULL));
		first = false;
	}
	return min + rand() % ((max + 1) - min);
}
