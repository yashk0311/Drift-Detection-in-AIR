#include <bits/stdc++.h>
using namespace std;

vector<string> random_sample(vector<string> &items, int num)
{
    random_shuffle(items.begin(), items.end());
    vector<string> result(items.begin(), items.begin() + num);
    return result;
}

int main()
{
    ofstream file("generator.txt");

    vector<string> items = {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15"};
    int throughput = 1;
    int drift_rate = 4;
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
    bool pause = false;

    while (true)
    {
        // Generate a bag of trending items
        vector<string> bag = random_sample(trending_items, 5);
        sort(bag.begin(), bag.end());
        // cout << "Bag " << bag_count + 1 << ": ";
        for (auto &item : bag)
        {
            // cout << item << " ";
            file << item << " ";
        }
        file << endl;
        // cout << endl;
        bag_count += 1;
        this_thread::sleep_for(chrono::seconds(1) / throughput); // Wait for the next bag

        if (pause && chrono::duration_cast<chrono::seconds>(chrono::system_clock::now() - pause_start).count() <= 3)
        {
            continue;
        }
        else if (pause && chrono::duration_cast<chrono::seconds>(chrono::system_clock::now() - pause_start).count() > 3)
        {
            pause = false;
            replaced_items.clear();  
            remaining_items = items;
            for (const auto &item : trending_items)
            {
                remaining_items.erase(remove(remaining_items.begin(), remaining_items.end(), item), remaining_items.end());
            }
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
            cout << "Change count: " << change_count << endl;
            if (change_count == 5)
            {
                // After all trending items have been changed, start the pause
                pause_start = chrono::system_clock::now();
                pause = true;
                change_count = 0;
            }
        }
    }
    file.close();
    return 0;
}