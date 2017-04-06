#undef TRACE
/*
 * Given a vector of cache hit ratio (hits per request) for each of N nodes,
 * and knowing which of them is the current node, our goal is to return a
 * random vector of C different nodes (i.e., a combination of C out of N),
 * where the goals of the random distribution are:
 *
 * 1. If we send each request to the C returned nodes, the *misses per
 *    second* of all nodes will be the same. In other words, nodes with
 *    low hit ratios will be sent less work.
 *
 * 2. We know that this node is one of the N nodes. As much as possible,
 *    without breaking goal 1, we should return this node as one of the
 *    results.
 *
 * 3. We assume that *this node* got chosen uniformly randomly among the
 *    N nodes (in other words, the client chose a coordinator node, us,
 *     uniformly, and we need to choose C nodes and forward the request
 *     to them).
 */
#include <vector>
#include <list>
#include <utility>
#include <random>
#include <algorithm>

#include <cassert>

#include <iostream>

#ifdef TRACE
namespace std {
template <typename T>
static inline
std::ostream& operator<<(std::ostream& os, const std::vector<T>& v) {
    bool first = true;
    os << "{";
    for (auto&& elem : v) {
        if (!first) {
            os << ", ";
        } else {
            first = false;
        }
        os << elem;
    }
    os << "}";
    return os;
}
template <typename T>
static inline
std::ostream& operator<<(std::ostream& os, const std::list<T>& v) {
    bool first = true;
    os << "{";
    for (auto&& elem : v) {
        if (!first) {
            os << ", ";
        } else {
            first = false;
        }
        os << elem;
    }
    os << "}";
    return os;
}
template <typename T1, typename T2>
static inline
std::ostream& operator<<(std::ostream& os, const std::pair<T1, T2>& v) {
    os << "{" << v.first << ", " << v.second << "}";
    return os;
}
}
#endif

class rand_exception {};


// randone() takes a vector of N integers and a probability for each, and
// returns one of these integers, according to the desired probabilities.
// If the given probabilities do not sum to 1, they are normalized accordingly.
//
// This implementation has complexity O(N). If we plan to call randone()
// many times on the same input and N can grow large, we should consider a
// different implementation, known as "the alias method", which has O(N)
// preperation stage (unavoidable, given our input is a vector of N items),
// but then only O(1) for each call.
// 
// “The Alias Method” was first suggested by A.J. Walker in 1977 and later
// refined by Knuth and others. The Alias method is explained in
// https://prxq.wordpress.com/2006/04/17/the-alias-method/. Here is my
// version of this explanation:
// The traditional method implemented by randone() divides the interval [0,1)
// into consecutive intervals of length Pi (where sum of Pi is 1), then picks
// a random point [0,1) and checks which of the intervals it covers. The
// observation behind the Alias Method is that the same technique will
// continue to work if we take these intervals and rearrange and/or cut them
// up as long as we keep their total lengths. The goal would be to cut them
// up in such a way that it makes it easy (O(1)) to find which interval is
// underneath each point we pick on [0,1).
// To do this, we begin by dividing [0,1) to N intervals of equal length 1/N,
// and then packing in each of those at most two intervals belonging to
// different i’s. Now, to find to which i a point belongs to, all we need
// to do is to find in which of the equal-length interval it is (a trivial
// division and truncation), and then finding out which one of the two
// possibilities that are left holds (one comparison).
// How do we pack the equal-length 1/N intervals correctly? We begin by
// putting in the first one a Pi such that Pi <= 1/N (always possible,
// of course). If the inequality was strict, so Pi did not completely fill
// the first 1/N-length interval, we pick another Pj where Pj >= 1/N (again,
// possible), take away from it what is needed to fill up the 1/N-length
// interval, reducing Pj for the rest of the algorithm. Now, we continue
// the same algorithm with one interval less and one less value, so it will
// end in O(N) time.
// Various variations of and optimizations of the Alias Method are known,
// see also
// https://web.archive.org/web/20131029203736/http://web.eecs.utk.edu/~vose/Publications/random.pdf


const std::pair<unsigned, float>& randone(const std::vector<std::pair<unsigned, float>>& ip) {
    float sum = 0.0;
    for (auto& t : ip) {
        sum += t.second;
    }
    float f = drand48() * sum;
    for (auto& t : ip) {
        // TODO: no need to do this for the last one ;-)
        f -=  t.second;
        if (f < 0) {
            return t;
        }
    }
    // can't be, if sum of p is sum
#ifdef TRACE
    std::cout << "randone1 exception\n";
#endif
    throw rand_exception();
}
// Same, with different interface. Assumes (but doesn't check) sum of p is
// 1.0.
// TODO: leave just one randone() implementation.
unsigned randone(const std::vector<float>& p,
        float rnd = drand48()) {
    unsigned n = p.size();
    for (unsigned i = 0; i < n - 1; i++) {
        rnd -=  p[i];
        if (rnd < 0) {
            return i;
        }
    }
    // TODO: confirm that rnd is 0, or at least very close to 0? Otherwise
    // p didn't sum up to 1...
    return n - 1;
}


// Function for producing random combinations (i.e., unordered subset) of
// length K out of N items 0..N-1.
// Returns a vector<int> with size K whose items are different integers
// between 0 and N-1.
//
// TODO: we can make this a template of K, N and have specialized
// implementations for low N (e.g., 3) or K=N-1, etc.
// TODO: write to a pre-allocated return vector to avoid extra allocation
//
// This code is an incomplete attempt at giving each *combination* of K
// items a probability (calculated from the probability of each item),
// and then drawing a random combinaton. Currently, only the case of K = N-1
// is implemented, where drawing a combination is as simple as drawing one
// item which will be left *out* of the combination.
std::vector<unsigned>
randcomb2(unsigned k, const std::vector<float>& p) {
    auto n = p.size();
    if (k > p.size()) {
#ifdef TRACE
        std::cout << "randcomb2 exception\n";
#endif
        throw rand_exception();
    } else if (k == 0) {
        return std::vector<unsigned>();
    }
    std::vector<unsigned> ret;
    ret.reserve(k);
    if (k == n) {
        // NOTE: In this case we do not fulfill the desired p.
        // return an error?
        std::cerr << "randcomb2: can't match p. case 1.\n";
        for (unsigned i = 0; i < n; i++) {
            ret.push_back(i);
        }
        return ret;
    } else if (k == n - 1) {
        // In this case, there are exactly k results, each result is a
        // combination x_i which misses exactly item i, so
        //
        // 1 - p(x_i) = sum_j!=i p(x_j) = k p_i
        std::vector<std::pair<unsigned,float>> np;
        np.reserve(n);
        for (unsigned i = 0; i < n; i++) {
            // TODO: If negative, throw
            float q = 1 - k*p[i];
            if (q < 0) {
                //std::cerr << "randcomb2: can't match p. case 2. q=" << q << "\n";
                q = 0; 
            }
            else if (q > 1) {
                //std::cerr << "randcomb2: can't match p. case 2. q=" << q << "\n";
                q = 1; 
            }
            np.emplace_back(i, q);
        }
        auto d = randone(np).first;
        std::vector<unsigned> ret;
        ret.reserve(k);
        for (unsigned i = 0; i < n; i++) {
            if (i != d) {
                ret.push_back(i);
            }
        }
        return ret;
    }
    // TODO
#ifdef TRACE
    std::cout << "randcomb2a exception\n";
#endif
    throw rand_exception();
}

// Function for producing random combinations (i.e., unordered subset) of
// length K out of N items 0..N-1.
// Returns a vector<int> with size K whose items are different integers
// between 0 and N-1.
//
// A vector of N probabilities is given, whose sum should be 1.0.
// The meaning of a probability p_i is that if we look at all the items
// returned in all the returned combinations, a fraction p_i of those will
// be item i. Note that p_i should not be higher than 1/K: even if we return
// item i in *every* K-combination, item i will still be only 1/K of the
// produced items... To reach p_i > 1/K will mean some combinations will
// need to contain more than one copy of i - which contradicts the defintion
// of a "combination".
//
// As our incomplete attempt with randcomb2() shown, it is difficult to
// fulfill both the 1st order inclusion probabilities (the given p vector)
// and high-order inclusion probabilities (i.e., that the probability for
// a pair of items, or a specific combination of items, is as can be
// calculated by assuming the independence of several random draws).
//
// Therefore, randcomb3() attempts to fulfill *only* the 1st order inclusion
// probabilities, foregoing any guarantees on high order inclusion
// probabilities. In other words, the individual items may not be independent.
// To understand what this means, consider a simple example: we have 4 items
// with equal probability (N=4) and want to draw random pairs (K=2).
// We can return {0,1} half of the time, and {2,3} the other half of the
// time, and achieve the desired probabilities (each item will be given 1/4
// of the work), but the pair {1,2}, for example, will never appear in any
// individual draw.
// In our use case, fulfilling *only* the 1st order inclusion probabilities
// is enough, because this is all we really need: We want that each node
// gets a given amount of work, but don't care if the different K nodes we
// choose in one request are correlated.
//
// A simple method of reproducing a set of desired 1st-order inclusion
// probabilities is Systematic Sampling, see definitions in
//     * https://en.wikipedia.org/wiki/Inclusion_probability
//     * https://en.wikipedia.org/wiki/Systematic_sampling
// and a very good explanation how to use it in
// http://stats.stackexchange.com/questions/139279/systematic-sampling-with-unequal-probabilities
// The "Systematic Sampling" technique is a simple extension of the randone()
// algorithm above. Both start by putting the given probabilities one after
// another on the segment [0,1). randone() then drew one random number in
// [0,1) and looked on which of the segments this point falls. Here, we draw
// a random number x in [0, 1/K), look at it, but then look x+1/K, x+2/K...
// and these produce K different items (the items must be different because
// of our assumption that none of the p_i are larger than 1/K), and the
// probability to choose each item is exactly p_i*K (which is equivalent
// to saying that the item's overall share is p_i, as we desire).
//
// randcomb3() only calls for one random number generation (which is
// important for performance) but calls randone() on the same vector K
// times, which makes it even more interesting to implement the Alias Method
// described above (of course, for very small N like 3, the difference is not
// interesting).
// TODO: If k == n-1, should we fall back to randcomb2? It is slightly more
// efficient because it only calls randone() once, not k times.
std::vector<int>
randcomb3(unsigned k, const std::vector<float>& p) {
    const float interval = 1.0 / k;
    const float rnd = drand48() * interval;  // random number in [0, 1/k)
    std::vector<int> ret;
    ret.reserve(k);
    float offset = 0;
    for (unsigned i = 0; i < k; i++) {
#ifdef TRACE
        std::cout << "randcomb3 " << i << " " << rnd << " " << offset << "\n";
#endif
        ret.emplace_back(randone(p, rnd + offset));
        offset += interval;
    }
    //std::cout << "ret " << ret << "\n";
    return ret;
}

template<typename Node>
class combination_generator {
private:
    std::vector<float> pp;
    std::vector<Node> nodes;
    int c;
public:
    combination_generator(std::vector<float>&& pp, std::vector<Node>&& nodes, int c)
        : pp(std::move(pp)), nodes(std::move(nodes)), c(c) {
        // TODO: throw if pp.size() != nodes.size() or not 1 <= c < pp.size()
    }
    std::vector<Node> get() {
        std::vector<Node> ret;
        ret.reserve(c);
        //std::vector<int> r = randcomb2(c, pp);
        std::vector<int> r = randcomb3(c, pp);
        for (int i : r) {
            ret.push_back(nodes[i]);
        }
        return ret;
    }
};




template<typename Node>
#if 0
std::vector<Node>
#else
combination_generator<Node>
#endif
miss_equalizing_combination(
    const std::vector<std::pair<Node,float>>& node_hit_rate, unsigned me, int bf)
{
    assert(me < node_hit_rate.size());

    static thread_local std::default_random_engine random_engine;

    struct ep_info {
        Node ep;
        float ht;
        float p;
    };

    float mr_sum = 0;
    float psum = 1;
    auto rf = node_hit_rate.size();

    std::vector<ep_info> epi;
    for (const auto& node : node_hit_rate) {
        mr_sum += 1/(1.0f - node.second);
        // initially probability of each node is 1/rf, but may be recalculated later
        epi.push_back({node.first, node.second, psum/rf});
    }
    float diffsum = 0;
    float restsum = 0;
    psum = 0;

    // recalculate p and psum according to hit rates
    for (auto&& ep : epi) {
        ep.p = 1 / (1.0f - ep.ht) / mr_sum;
        psum += ep.p;
        // shoehorn probabilities to be not greater than 1/CL
        if (ep.p > 1.0f / bf) {
            diffsum += (ep.p - 1.0f / bf);
            ep.p = 1.0f / bf;
        } else {
            restsum += ep.p;
        }
    }

    float D = 0; // total deficit
    float Dtag = 0;
    for (auto&& ep : epi) {
        // redistribute everything above 1/CL
        if (ep.p < 1.0f / bf) {
            ep.p += (ep.p * diffsum / restsum);
        }
        auto x = rf * ep.p - 1.0f / bf;
        if (x >= 0) {
            D += x;
        } else {
            Dtag += (1.0f - rf * ep.p);
        }
    }

#ifdef TRACE
    std::cout << "desired probabilities:\n";
    for (unsigned i = 0; i < epi.size(); i++) {
        std::cout << epi[i].ep << ": " << epi[i].p <<
            ((i==me) ? " (coordinator)\n" : "\n");
    }
#endif


    std::vector<float> pp(rf);
    // "Keep for node i"
    // A surplus node keeps its entire desired amount of request, N*p,
    // for itself. A mixed node is cut off by 1/C.
    pp[me] = std::min(rf * epi[me].p, 1.0f / bf);
#ifdef TRACE
    std::cout << "pp[me(" << me << ")]  = " << pp[me] << "\n";
#endif

// In 0.81,0.79,0.27 i.e., p = 0.461,0.417,0.12 we get:
//           p          surplus      deficit
// mixed   0.461        0.5          0.883
// mixed   0.417        0.5          0.751
// surplus 0.12         0.5          0
//
// We cannot normalize the mixed deficits to equal the mix surplus (1)
// because then we get 0.54, 0.459 - and 0.459 is lower than the other
// node's surplus (0.5) so we wouldn't be able to close all of it!
//
// So we can't normalize like we did before.
// And if we don't normalize, we also don't know what is the remaining
// deficit without performing the entire mixed-nodes algorithm, even
// if me is a suplus node!
// So we just run the mixed node algorithm in every case, regardless of
// what this node is. We need to remember the deficit left at its
// end.

    std::vector<float> deficit(rf);
    float total_deficit = 0;
    int mixed_count = 0;
    for (unsigned j = 0; j < rf; j++) {
        float NPj = rf * epi[j].p;
        float deficit_j = NPj - 1.0f / bf;
        if (deficit_j >= 0) {
            // mixed node
            mixed_count++;
            deficit[j] = deficit_j;
            total_deficit += deficit_j;
        }
    }
    // Each of the mixed nodes have the same same surplus:
    float mixed_surplus = 1 - 1.0f / bf;

#ifdef TRACE
    std::cout << "mixed_count " << mixed_count << "\n";
    std::cout << "starting distribution of mixed-node surplus to other mixed nodes:\n";
    std::cout << "deficit " << deficit << "\n";
    std::cout << "mixed_surplus " << mixed_surplus << "\n";
#endif

    float my_surplus;
    if (deficit[me] == 0) {
        // surplus node
        my_surplus = 1 - rf * epi[me].p;
    } else {
        // mixed node, which will be converted below to either a deficit
        // node or a surplus node. We can easily calculate now how much
        // surplus will be left. It will be useful to know below if "me"
        // will be a surplus node, because we only need to know how much
        // work "me" *sends*, so if me is not a surplus node, we won't need
        // to do the second step (of distributing surplus to the deficit
        // nodes), and won't even need to update deficit[].
        if (deficit[me] <= mixed_surplus) {
            // Node will be converted to a surplus node
            my_surplus = mixed_surplus - deficit[me];
        } else {
            // Node will be converted to a deficit node, and will not be
            // left with any surplus
            my_surplus = 0;
        }
    }
#ifdef TRACE
    std::cout << "my_surplus " << my_surplus << "\n";
#endif

    // Mixed node redistribution algorithm, to "convert" mixed nodes into
    // pure surplus or pure deficit nodes, while flowing probability between
    // the mixed nodes (we only need to track this flow here if "me" is the
    // node doing the sending - in pp[]).
    if (deficit[me]) {
        // "me" is a mixed node. 
#ifdef TRACE
        std::cout << "CASE1\n";
#endif
        // We need a list of the mixed nodes sorted in increasing deficit order.
        // Actually, we only need to sort those nodes with deficit <=
        // min(deficit[me], mixed_surplus).
        // TODO: use NlgN sort instead of this ridiculous N^2 implementation.
        // TODO: can we do this without a NlgN (although very small N, not even
        // the full rf)? Note also the distribution code below is N^2 anway
        // (two nested for loops).
        std::list<std::pair<unsigned, float>> sorted_deficits;
        for (unsigned i = 0; i < rf; i++) {
            if (deficit[i] && deficit[i] <= deficit[me] &&
                    deficit[i] < mixed_surplus) {
                auto it = sorted_deficits.begin();
                while (it != sorted_deficits.end() && it->second < deficit[i])
                    ++it;
                sorted_deficits.insert(it, std::make_pair(i, deficit[i]));
            }
        }
#ifdef TRACE
        std::cout << "sorted_deficits " << sorted_deficits << "\n";
#endif
        float s = 0;
        int count = mixed_count;
        for (auto &d : sorted_deficits) {
#ifdef TRACE
            std::cout << "next sorted deficit: " << d << "\n";
#endif
            // What "diff" to distribute
            auto diff = d.second - s;
            s = d.second;
            if (!diff) {
                continue;
            }
#ifdef TRACE
            std::cout << "     diff: " << diff << "\n";
            std::cout << "     pp before: " << pp << "\n";
            std::cout << "     count: " << count << "\n";
#endif
            // Distribute it among all the mixed nodes with higher deficit
            // there should be exactly count of those including me.
            for (unsigned i = 0; i < rf; i++) {
#ifdef TRACE
                std::cout << i << " " << d.first << " " << deficit[i] << " " << d.second << "\n";
#endif
                // TODO: think here - is >= ok?
                if (i != me && deficit[i] >= d.second) {
                    pp[i] += diff / (count - 1);
#ifdef TRACE
                    std::cout << "pp[" << i << "] = " << pp[i] << " (case a)\n";
#endif
                }
            }

#ifdef TRACE
            std::cout << "     pp after1: " << pp << "\n";
#endif
            // TODO: confirm last loop really had "count" success iterations.
            --count;
            if (d.first == me) {
                // We only care what "me" sends, and only the elements in
                // the sorted list earlier than me could have forced it to
                // send, so the rest of the algorithm isn't interesting.
                break;
            }
        }
        // additionally, if me is converted to a deficit node, we need to
        // take the remaining surplus (mixed_surplus minus the last deficit
        // in sorted_deficits) and distribute it to the other count-1
        // converted-to-surplus nodes. Of course we can only do this if
        // count > 1 - if count==1, we remain with just one mixed node
        // and cannot eliminate its surplus without "fixing" some of the
        // decisions made earlier
        if (deficit[me] > mixed_surplus) {
            auto last_deficit = sorted_deficits.back().second;
            auto diff = mixed_surplus - last_deficit;
            if (count > 1) {
#ifdef TRACE
                std::cout << "CASE4. surplus " << diff << " count " << count << "\n";
#endif
                for (unsigned i = 0; i < rf; i++) {
                    if (i != me && deficit[i] > last_deficit) {
#ifdef TRACE
                        std::cout << "adding " << (diff / (count-1)) << " to pp[" << i << "] = " << pp[i] << "\n";
#endif
                        pp[i] += diff / (count - 1);
                    }
                }
                // TODO: confirm that this loop worked exactly count - 1 times.
            } else {
#ifdef TRACE
                std::cout << "CASE3a. surplus " << diff << "\n";
#endif
                // CASE3: count == 1 is possible. example for p = 0.2, 0.3, 0.5:
                //    surplus  0.5  0.5  0.5
                //    deficit  0.1  0.4  1.0
                // after first step redistributing 0.1 to 3 nodes:
                //    surplus  0.4  0.4  0.4
                //    deficit  0.0  0.3  0.9
                // after first step redistributing 0.3 to 2 nodes:
                //    surplus  0.4  0.1  0.1
                //    deficit  0.0  0.0  0.6
                // So we're left with 1 mixed node (count=1), and can't
                // redistribute its surplus to itself!
                // This happens because the original distribution step was
                // already a mistake: In this case the *only* solution is for node
                // 0 and 1 is to send all their surplus (total of 1.0) to fill
                // node 2's entire deficit (1.0). Node 0 can't afford to send
                // any of its surplus to node 1 - and if it does (like we did in
                // the first step redistributing 0.1), we end up with
                // deficit remaining on node 2!
                //
                // Special case of one remaining mixed node. Tell the other
                // nodes not to give each other as much (we don't have to
                // do this here, as we only care about "me") and instead
                // "me" will give them their surplus
                for (unsigned i = 0; i < rf; i++) {
                    if (i != me) {
                        pp[i] += diff / (mixed_count - 1);
#ifdef TRACE
                        std::cout << "pp[" << i << "] = " << pp[i] << "(case b)\n";
#endif
                    }
                }
            }
#ifdef TRACE
            std::cout << "     pp after2: " << pp << "\n";
#endif
        } else {
            // Additionally, if the algorithm ends with a single mixed node
            // we need to apply a fix. Above we already handled the case that
            // this single mixed node is "me", so it needs to send more to the
            // other nodes. Here we need to handle the opposite side - me is
            // one of the nodes which sent too much to other nodes and needs
            // to send to the mixed node instead.
            // TODO: find a more efficient way to check if the alorithm will
            // end with just one mixed node and its surplus :-(
            unsigned n_converted_to_deficit = 0;
            unsigned mix_i = 0; // only used if n_converted_to_deficit==1
            float last_deficit = 0;
            for (unsigned i = 0; i < rf; i++) {
                if (deficit[i] > mixed_surplus) {
                    n_converted_to_deficit++;
                    mix_i = i;
                } else {
                    last_deficit = std::max(last_deficit, deficit[i]);
                }
            }
            if (n_converted_to_deficit == 1) {
                auto diff = mixed_surplus - last_deficit;
#ifdef TRACE
                std::cout << "CASE3b. surplus " << diff << "\n";
#endif
                pp[mix_i] += diff / (mixed_count - 1);
#ifdef TRACE
                std::cout << "pp[" << mix_i << "] = " << pp[mix_i] << "(case c)\n";
#endif
                for (unsigned i = 0; i < rf; i++) {
                    if (deficit[i] > 0) { // mixed node
                        if (i != mix_i && i != me) {
                            pp[i] -= diff / (mixed_count - 1) / (mixed_count - 2);
#ifdef TRACE
                            std::cout << "pp[" << i << "] = " << pp[i] << "(case d)\n";
#endif
                        }
                    }
                }
            }
        }
    }

    if (my_surplus) {
        // "me" is a surplus node, or became one during the mixed node
        // redistribution algorithm.  We need to know the new deficit nodes
        // produced by that algorithm. i.e., we need to update deficit[].
        float new_total_deficit = 0;
        for (unsigned i = 0; i < rf; i++) {
            if (deficit[i] > 0) {
                // Mixed node.
                if (deficit[i] > mixed_surplus) {
                    // The mixed-node redistribution algorithm converted it
                    // to a deficit node, with this deficit:
                    deficit[i] -= mixed_surplus;
                    new_total_deficit += deficit[i];
                } else {
                    // The mixed-node redistribution algorithm converted it
                    // to a surplus node, with no deficit:
                    deficit[i] = 0;
                }
            }
        }
        // Split "me"'s surplus to the other nodes' remaining deficit,
        // according to their share in the total remaining deficit.
        for (unsigned j = 0; j < rf ; j++) {
            if (deficit[j] > 0) {
                // note j!= me because surplus node has deficit==0.
                // Note pp[j] +=, not =, because this node might have
                // already flowed some work to other nodes in the
                // mixed node redistribution algorithm above.
                pp[j] += deficit[j] / new_total_deficit * my_surplus; 
#ifdef TRACE
                std::cout << "pp[" << j << "] = " << pp[j] << "(case e)\n";
#endif
            }
        }
    }

#ifdef TRACE
    std::cout << "returned pp: " << pp << "\n";
#endif
    std::vector<Node> nodes(rf);
    for (unsigned i = 0; i < rf; i++) {
        nodes[i] = epi[i].ep;
    }
    return combination_generator<Node>(std::move(pp), std::move(nodes), bf);
}

