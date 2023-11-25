#include <unistd.h>
#include "../serialization/Serialization.hpp"
#include "EventCollector_cd.hpp"
#include <bits/stdc++.h>
#include <chrono>
#include <fstream>
#include <istream>
using namespace std;

EventCollectorRP::EventCollectorRP(int tag, int rank, int worldSize) : Vertex(tag, rank, worldSize), 
lat("/home/yash/Desktop/Clg/7th_Sem/SDS/Assignment_3/AIR_regex/latencies.csv"), 
tp("/home/yash/Desktop/Clg/7th_Sem/SDS/Assignment_3/AIR_regex/throughputs.csv")
{

	// Global stats
	sum_latency = 0;
	sum_counts = 0;
	num_messages = 0;

	S_CHECK(if (rank == 0) {
		datafile.open("Data/results" + to_string(rank) + ".tsv");
	})

	D(cout << "EventCollectorRP [" << tag << "] CREATED @ " << rank << endl;)
}

EventCollectorRP::~EventCollectorRP()
{
	D(cout << "EventCollectorRP [" << tag << "] DELETED @ " << rank << endl;)
}

void EventCollectorRP::batchProcess()
{
	D(cout << "EventCollectorRP->BATCHPROCESS [" << tag << "] @ " << rank << endl;)
}

long int EventCollectorRP::getCurrentTimestampMillisec()
{
	using namespace std::chrono;

	auto currentTimePoint = system_clock::now();
	auto duration = duration_cast<milliseconds>(currentTimePoint.time_since_epoch());

	return duration.count();
}
void EventCollectorRP::streamProcess(int channel)
{
	D(cout << "EventCollectorRP->STREAMPROCESS [" << tag << "] @ " << rank
		   << " IN-CHANNEL " << channel << endl;)

	if (rank == 0)
	{
		Message *inMessage;
		list<Message *> *tmpMessages = new list<Message *>();
		Serialization sede;

		EventReg eventPCReg;

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

				D(cout << "EVENTCOLLECTOR->POP MESSAGE: TAG [" << tag << "] @ "
					   << rank << " CHANNEL " << channel << " BUFFER "
					   << inMessage->size << endl);

				int event_count = inMessage->size / sizeof(EventReg);

				int i = 0, count = 0;
				while (i < event_count)
				{
					sede.YSBdeserializePCReg(inMessage, &eventPCReg,
											 i * sizeof(EventReg));
					sum_latency += eventPCReg.latency;
					count += eventPCReg.count;
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

				lat << WID_no << ", " << eventPCReg.latency << endl;
				tp << WID_no << ", " << eventPCReg.count << endl;

				cout << "The WID is: " <<eventPCReg.WID << endl;
				cout << "Latency of this window: " << eventPCReg.latency << endl;
				cout << "The number of events in that window id is: " << count << endl;
				cout << "Total latency uptil this window is: " << sum_latency << endl;
				cout << "------------------------------------------------------" << endl;
				delete inMessage; // delete message from incoming queue
				c++;
				WID_no++;
			}

			tmpMessages->clear();
		}

		delete tmpMessages;
	}
}
