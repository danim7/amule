// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (c) 2026 aMule Team

#ifndef DIAGDRAINCASE_H
#define DIAGDRAINCASE_H

#include <algorithm>
#include <cstdlib>
#include <map>
#include <random>
#include <string>
#include <vector>

// EXPERIMENT (#2607), not for upstream: how an upload slot keeps a slowly draining peer.
// > 0: that TCP_NOTSENT_LOWAT mark; 0: nothing, as on master; kQueueCheck: the kernel queue check.
namespace DiagDrainCase
{

constexpr int kQueueCheck = -1;
constexpr int kNone = -2; // no TCP upload slot

inline std::string Name(int drainCase)
{
	switch (drainCase) {
	case 0:
		return "off";
	case kQueueCheck:
		return "queue";
	case kNone:
		return "none";
	default:
		return std::to_string(drainCase);
	}
}

// Every peer walks through all the cases, one per upload slot, so the same peer is measured with
// each. The case a new peer starts with comes from shuffled rounds of all cases, which keeps the
// cases balanced among the many peers that only ever get one slot. Main thread only.
class CRotation
{
public:
	// spec: comma separated "off", "queue" or a mark in bytes. One entry is the same case for all.
	// Nothing usable in it means the default.
	explicit CRotation(const char *spec)
	: m_cases(Parse(spec ? spec : ""))
	{
		if (m_cases.empty()) {
			m_cases = { 0, 16384, 32768, kQueueCheck };
		}
	}

	int Next(const std::string &peer)
	{
		auto it = m_peers.find(peer);
		if (it == m_peers.end()) {
			it = m_peers.emplace(peer, NextStart()).first;
		} else {
			it->second = (it->second + 1) % m_cases.size();
		}
		return m_cases[it->second];
	}

	std::string Names() const
	{
		std::string names;
		for (const int drainCase : m_cases) {
			names += (names.empty() ? "" : ",") + Name(drainCase);
		}
		return names;
	}

private:
	static std::vector<int> Parse(const std::string &spec)
	{
		std::vector<int> cases;
		for (size_t pos = 0; pos <= spec.size();) {
			const size_t end = std::min(spec.find(',', pos), spec.size());
			const std::string entry = spec.substr(pos, end - pos);
			pos = end + 1;
			if (entry == "off") {
				cases.push_back(0);
			} else if (entry == "queue") {
				cases.push_back(kQueueCheck);
			} else if (const int mark = std::atoi(entry.c_str()); mark > 0) {
				cases.push_back(mark);
			}
		}
		return cases;
	}

	size_t NextStart()
	{
		if (m_round.empty()) {
			for (size_t i = 0; i < m_cases.size(); ++i) {
				m_round.push_back(i);
			}
			std::shuffle(m_round.begin(), m_round.end(), m_random);
		}
		const size_t start = m_round.back();
		m_round.pop_back();
		return start;
	}

	std::vector<int> m_cases;
	std::vector<size_t> m_round;
	std::map<std::string, size_t> m_peers;
	std::mt19937 m_random{ std::random_device{}() };
};

} // namespace DiagDrainCase

#endif // DIAGDRAINCASE_H
