#include "EventFilter_cd.hpp"

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

EventFilterRP::EventFilterRP(int tag, int rank, int worldSize, string pattern) : Vertex(tag, rank, worldSize)
{
	this->pattern = pattern;
	D(cout << "EventFilterRP [" << tag << "] CREATED @ " << rank << endl;)
	THROUGHPUT_LOG(
		datafile.open("Data/tp_log" + to_string(rank) + ".tsv");)
}

EventFilterRP::~EventFilterRP()
{
	D(cout << "EventFilterRP [" << tag << "] DELETED @ " << rank << endl;)
}

void EventFilterRP::batchProcess()
{
	cout << "EventFilterRP->BATCHPROCESS [" << tag << "] @ " << rank << endl;
}

bool EventFilterRP::find_regex(string text, string pattern)
{
	regex regexPattern(pattern);
	smatch match;

	return regex_search(text, match, regexPattern);
}

void EventFilterRP::streamProcess(int channel)
{

	D(cout << "EventFilterRP->STREAMPROCESS [" << tag << "] @ " << rank
		   << " IN-CHANNEL " << channel << endl;)

	Message *inMessage, *outMessage;
	list<Message *> *tmpMessages = new list<Message *>();
	Serialization sede;

	// WrapperUnit wrapper_unit;
	EventRG eventRG;
	EventFT eventFT;

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

			D(cout << "EventFilterRP->POP MESSAGE: TAG [" << tag << "] @ " << rank
				   << " CHANNEL " << channel << " BUFFER " << inMessage->size
				   << endl;)

			sede.unwrap(inMessage);

			int offset = sizeof(int) + (inMessage->wrapper_length * sizeof(WrapperUnit));

			outMessage = new Message(inMessage->size - offset,
									 inMessage->wrapper_length); // create new message with max. required capacity

			memcpy(outMessage->buffer, inMessage->buffer, offset); // simply copy header from old message for now!
			outMessage->size += offset;

			int event_count = (inMessage->size - offset) / sizeof(EventDG);

			D(cout << "THROUGHPUT: " << event_count << " @RANK-" << rank << " TIME: " << (long int)MPI_Wtime() << endl;)

			THROUGHPUT_LOG(
				datafile << event_count << "\t"
						 << rank << "\t"
						 << (long int)MPI_Wtime()
						 << endl;)

			int i = 0, j = 0;
			while (i < event_count)
			{

				sede.YSBdeserializeRG(inMessage, &eventRG,
									  offset + (i * sizeof(EventRG)));

				string text = eventRG.ad_id;
				
				if (find_regex(text, pattern))
				{ // FILTERING BASED ON EVENT_TYPE
					eventFT.event_time = eventRG.event_time;
					memcpy(eventFT.ad_id, eventRG.ad_id, 51);
					sede.YSBserializeFT(&eventFT, outMessage); 
					j++;
				}

				i++;
			}

			// Replicate data to all subsequent vertices, do not actually reshard the data here
			int n = 0;
			for (vector<Vertex *>::iterator v = next.begin(); v != next.end();
				 ++v)
			{
				int wid = eventRG.event_time/AGG_WIND_SPAN;
				int idx =  n *  worldSize + 0; // always keep workload on same rank
				// int idx =  wid % worldSize + 0; // always keep workload on same rank
				
				if (PIPELINE)
				{

					// Pipeline mode: immediately copy message into next operator's queue
					pthread_mutex_lock(&(*v)->listenerMutexes[idx]);
					(*v)->inMessages[idx].push_back(outMessage);

					D(cout << "EventFilterRP->PIPELINE MESSAGE [" << tag << "] #"
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

					D(cout << "EventFilterRP->PUSHBACK MESSAGE [" << tag << "] #"
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
