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

// FIXME: Stability:
// The current mixed-node redistribution algorithm has a stability problem.
// Currently, if all hit rates are equal and CL=2, each node gives itself
// 0.5 and 0.5 to one other node. But we don't want node 0 to decide to
// give 0.5 to node 1, and then node 2 decides independently to give 0.5
// also to the same node 1 - node 2 should decide to give the 0.5 to node 0.
// This can be achieved by ensuring that all nodes run the algorithm on the
// same list of nodes in the same order, and make exactly the same decisions.
// However, we have a stability problem - what if node 0 thinks the hit
// ratios are 1.0 0.99 0.99 but node 2 thinks they are 1.01 1.0 1.0? They
// may again make conflicting decisions!
// The solution is for a node NOT to give out all its requests to one other
// node but rather spread them. That is stable - the spread changes only
// slightly if the input probabilities change slightly, rather than
// switching from giving everything to node 1 instead of everything to node
// 2 if one probability changes slightly.
// Example:
// N=3, CL=2, three equal hit ratios.
// In the solution, all three nodes will give 0.5 to themselves.
// 1. A non-stable solution (our current solution):
//    Node 0 gives 0.5 to itself, 0.5 to 1
//    Node 1 gives 0.5 to itself, 0.5 to 2
//    Node 2 gives 0.5 to itself, 0.5 to 0
//    So node 0 gives all its surplus work to node 1, assuming that node 1
//    and 2 will give their surplus to different nodes). But the problem is
//    that each node makes its decisions separately, so two nodes might
//    decide to send all their surplus to node 1 (for example).
// 2. A possible stable solution:
//    Node 0 gives 0.5 to itself, 0.25 to 1, 0.25 to 2
//    Node 1 gives 0.5 to itself, 0.25 to 0, 0.25 to 2
//    Node 2 gives 0.5 to itself, 0.25 to 0, 0.25 to 1
//
#include <vector>
#include <utility>
#include <random>
#include <algorithm>

#include <cassert>

#include <iostream>

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


const std::pair<int, float>& randone(const std::vector<std::pair<int, float>>& ip) {
    float sum = 0.0;
    for (auto& t : ip) {
        sum += t.second;
    }
    float f = drand48() * sum;
    for (auto& t : ip) {
        // TODO: no need to do this for the last one ;-)
        f -=  t.second;
        if (f <= 0) {
            return t;
        }
    }
    // can't be, if sum of p is sum
    throw rand_exception();
}
// Same, with different interface. Assumes (but doesn't check) sum of p is
// 1.0.
// TODO: leave just one randone() implementation.
int randone(const std::vector<float>& p,
        float rnd = drand48()) {
    //std::cout << "rnd = " << rnd << ", p= " << p << "\n";
    int n = p.size();
    for (int i = 0; i < n; i++) {
        // TODO: no need to do this for the last one ;-)
        rnd -=  p[i];
        if (rnd <= 0) {
            return i;
        }
    }
    // can't be, if sum of p is 1
    throw rand_exception();
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
std::vector<int>
randcomb2(unsigned k, const std::vector<float>& p) {
    auto n = p.size();
    if (k > p.size()) {
        throw rand_exception();
    } else if (k == 0) {
        return std::vector<int>();
    }
    std::vector<int> ret;
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
        std::vector<std::pair<int,float>> np;
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
        unsigned d = randone(np).first;
        std::vector<int> ret;
        ret.reserve(k);
        for (unsigned i = 0; i < n; i++) {
            if (i != d) {
                ret.push_back(i);
            }
        }
        return ret;
    }
    // TODO
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
        ret.emplace_back(randone(p, rnd + offset));
        offset += interval;
    }
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

//    std::cout << "epi:\n";
//    for(auto& e : epi) {
//        std::cout << "ep=" << e.ep << ", ht=" << e.ht << ", p=" << e.p << "\n";
//    }


    std::vector<float> pp(rf);
    // "Keep for node i"
    // A surplus node keeps its entire desired amount of request, N*p,
    // for itself. A mixed node is cut off by 1/C.
    pp[me] = std::min(rf * epi[me].p, 1.0f / bf);
    bool me_mixed = rf * epi[me].p >= 1.0f / bf;

// FIXME: CONTINUE HERE:
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
// But if we don't normalize, we also don't know what is the remaining
// deficit without performing the entire mixed-nodes algorithm, even
// if me is a suplus node!
// Let's just run the mixed node algorithm in every case, regardless of
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
//    std::cout << "mixed_count " << mixed_count << "\n";
//    std::cout << "deficit " << deficit << "\n";
    // Total surplus of mixed nodes only. Note that there are mixed_count
    // of those, and each one has surplus of exactly 1/ C
    float total_mixed_surplus = mixed_count * (1.0f - 1.0f / bf);
#if 0
    // The mixed node redistribution algorithm (we don't care about
    // its details here) removes a total of mixed_surplus_sum from
    // total_deficit - divided into equal shares.
    float factor = total_mixed_surplus / total_deficit;
    for (int j = 0; j < rf; j++) {
        deficit[j] = deficit[j] * factor;
    }
#endif
//    std::cout << "deficit2 " << deficit << "\n";

    // Participate in an algorithm to redistribute all the mixed nodes'
    // surplus to fill part of their deficit. Also, if this node is one
    // of the mixed nodes, while running this algorithm, remember in pp[]
    // what amount of work this node (me) sent to other nodes.
    // We need to run the same algorithm on all nodes to achieve
    // consistent decisions of who sends whom what.
    std::vector<float> tmp_surplus(rf);
    // Set about tmp_surplus for mixed nodes
    for (unsigned j = 0; j < rf; j++) {
        float NPj = rf * epi[j].p;
        float deficit_j = NPj - 1.0f / bf;
        if (deficit_j >= 0) {
            // mixed node
            tmp_surplus[j] = 1 - 1.0f / bf;
        }
    }
//    std::cout << "tmp_surplus=" << tmp_surplus << ", deficit=" << deficit << "\n";
    // Find the next node we want to fill its deficit, by picking the
    // one with the smallest deficit. Nodes that already have zero
    // deficit do not need filling, and are not picked.
    auto find_min_but_not_zero = [&] () {
        float m = std::numeric_limits<float>::infinity();
        int i = -1;
        for (unsigned j = 0; j < rf; j++) {
            if (deficit[j] < m && deficit[j] > 1e-5) {
                m = deficit[j];
                i = j;
            }
        }
        return i;
    };
    // Find a node which can contribute surplus to fill (partly) the
    // deficit in node "min_i" found by find_min_but_not_zero() above.
    // This has to be a different node, cannot be "min_i" again. We find
    // this node by taking the one with maximal deficit (a). However,
    // it cannot be a node whose surplus (b) is zero, otherwise it will
    // have nothing to contribute.
    auto find_max = [&] (int min_i) {
        float m = -std::numeric_limits<float>::infinity();
        int i = -1;
        for (int j = 0; j < (int)rf; j++) {
            if (j != min_i && deficit[j] > m && tmp_surplus[j] > 1e-5) {
                m = deficit[j];
                i = j;
            }
        }
        return i;
    };
    auto step = [&] () {
        // Look for highest and lowest deficit
        int min_i = find_min_but_not_zero();
        int max_i = find_max(min_i);
//        std::cout << "min_i = " << min_i << ", max_i=" << max_i << "\n";
        if (max_i == -1 && min_i == -1) {
//                std::cout << "success\n";
            return false; // success
        } else if (max_i == -1) {
            // FIXME: This is a hack for the hitrate case (0.81, 0.79, 0.27).
            // But we need to do something nicer.
            // we found min_i but then didn't find max_i. Probably
            // we chose as min_i the only viable option for max_i.
            // Try that
            max_i = min_i;
            if (tmp_surplus[max_i] < 1e-5) {
                return false; // nothing more worth doing
            }
            // find some large enough deficit
            min_i = -1;
            for (int j = 0; j < (int)rf; j++) {
                if (j != max_i && deficit[j] > tmp_surplus[max_i]) {
                    min_i = j;
                    break;
                }
            }
            if (max_i == -1 || min_i == -1) {
                std::cout << "fail3\n";
                return false;
            }
        } else if (max_i == -1 || min_i == -1) {
            // fail. FIXME: warn?
            std::cout << "fail1\n";
            return false;
        } else if (max_i == min_i) {
            // fail. bug?
            std::cout << "fail2\n";
            return false;
        }
        // TODO: if we already saw this min_i,max_i pair before we have
        // a bug. maybe an infinite loop. And better stop.

        // Give as much as of max_i's surplus to min_i.
        // TODO: consider if we always want to give everything. Perhaps
        // it makes sense to even things out instead of one node giving
        // everything to another node when the deficits are close? How?
        float exchange = std::min(tmp_surplus[max_i], deficit[min_i]);
        deficit[min_i] -= exchange;
        tmp_surplus[max_i] -= exchange;
        if (max_i == (int)me) {
            // Remember how much *this* node needs to send to other nodes.
            pp[min_i] += exchange;
        }
        return true;
    };
    while (step()) {
//            std::cout << "tmp_surplus=" << tmp_surplus << ", deficit=" << deficit << "\n";
    }

    // After redistributing the work of mixed nodes and "cancelling out"
    // all of their surplus, the mixed nodes become deficit-only nodes
    // (whose remaining deficit we already know in deficit[]) and, and
    // surplus nodes have surplus only. We now redistribute this using
    // the simple equal-share algorithm. Remember we only care about what
    // this node (0) gives out. 
    if (me_mixed) {
        // After the cancelling out of mixed nodes, this remains a deficit-
        // only node, and it has nothing more to send (beyond what it already
        // put in pp[] in the previous step).
    } else {
        // This is surplus-only node. We need to split its surplus to the
        // other nodes' remaining deficit, according to their share in the
        // total remaining deficit.
        float my_surplus = 1.0f - rf * epi[me].p;
        float new_total_deficit = total_deficit - total_mixed_surplus;
        for (unsigned j = 0; j < rf ; j++) {
            if (deficit[j] > 0) {
                // note j!= me because surplus node has deficit==0.
                pp[j] = deficit[j] / new_total_deficit * my_surplus; 
            }
        }
    }

//    std::cout << pp << "\n";
    std::vector<Node> nodes(rf);
    for (unsigned i = 0; i < rf; i++) {
        nodes[i] = epi[i].ep;
    }
    return combination_generator<Node>(std::move(pp), std::move(nodes), bf);
}

