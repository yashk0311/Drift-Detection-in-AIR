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
 * Aggregate.hpp
 *
 *  Created on: 18, Dec, 2018
 *      Author: vinu.venugopal
 */

#ifndef OPERATOR_FullAggregatorCD_HPP_
#define OPERATOR_FullAggregatorCD_HPP_

#include <unordered_map>
#include <utility>

#include "../dataflow/Vertex.hpp"

using namespace std;

typedef std::pair<int, long int> count_maxeventtime;
typedef unordered_map<long int, count_maxeventtime> InnerHMap;
typedef unordered_map<long int, std::pair<long int, long int>> OuterHMapReg;
typedef unordered_map<long int, std::pair<int, int>> WIDtoWrapperUnitHMap;

class FullAggregatorCD: public Vertex {

public:

	OuterHMapReg WIDtoIHM;
	pthread_mutex_t WIDtoIHM_mutex;

	WIDtoWrapperUnitHMap WIDtoWrapperUnit;
	pthread_mutex_t WIDtoWrapperUnit_mutex;

	FullAggregatorCD(int tag, int rank, int worldSize);

	~FullAggregatorCD();

	void batchProcess();

	void streamProcess(int channel);

};

#endif /* OPERATOR_FullAggregatorCD_HPP_ */
