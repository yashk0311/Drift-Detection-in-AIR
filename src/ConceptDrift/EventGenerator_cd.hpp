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
 * EventGeneratorCD.cpp
 *
 *  Created on: Dec  06, 2018
 *      Author: vinu.venugopal
 */

#ifndef CONNECTOR_EventGeneratorCD_HPP_
#define CONNECTOR_EventGeneratorCD_HPP_

#include "../dataflow/Vertex.hpp"
#include "../communication/Message.hpp"
#include "../serialization/Serialization.hpp"

using namespace std;
#include <fstream>

class EventGeneratorCD : public Vertex
{

public:
	EventGeneratorCD(int tag, int rank, int worldSize, unsigned long tp, unsigned long dr);

	~EventGeneratorCD();

	void batchProcess();

	void streamProcess(int channel);

private:
	unsigned long throughput;
	unsigned long drift_rate;

	std::ofstream datafile;

	vector<string> ad_ids;

	void getNextMessage(EventCD *event, WrapperUnit *wrapper_unit,
						Message *message, int events_per_msg, long int time_now, unsigned long tp, unsigned long dr);

	vector<string> random_sample(vector<string> &items, int num);
	int myrandom(int min, int max);

	string generate_bag(unsigned long tp, unsigned long dr);
	string eventtypes[3] = {"click", "view", "purchase"};
};

#endif /* CONNECTOR_EventGeneratorCD_HPP_ */
