#include "csv.h"
#include <filesystem>
#include <fstream>
#include <numeric>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "fmt/format.h"

struct wager_row
{
    int wager_id, begin_draw, end_draw;
    int qp;
    int ticket_cost, numbers_wagered_id, draw_number_id;
};

constexpr uint64_t BIT_LENGTH = 63;

std::unordered_map<int, std::unordered_map<int, int>> PRIZE_DICT = {
    { 10,
      { { 10, 100000 },
        { 9, 4250 },
        { 8, 450 },
        { 7, 40 },
        { 6, 15 },
        { 5, 2 },
        { 0, 5 } } },
    { 9, { { 9, 30000 }, { 8, 3000 }, { 7, 150 }, { 6, 25 }, { 5, 6 }, { 4, 1 } } },
    { 8, { { 8, 10000 }, { 7, 750 }, { 6, 50 }, { 5, 12 }, { 4, 2 } } },
    { 7, { { 7, 4500 }, { 6, 100 }, { 5, 17 }, { 4, 3 }, { 3, 1 } } },
    { 6,
      { { 6, 1100 },
        { 5, 50 },
        {
          4,
          8,
        },
        { 3, 1 } } },
    { 5, { { 5, 420 }, { 4, 18 }, { 3, 2 } } },
    { 4, { { 4, 75 }, { 3, 5 }, { 2, 1 } } },
    { 3, { { 3, 27 }, { 2, 2 } } },
    { 2, { { 2, 11 } } },
    {
      1,
      { { 1, 2 } },
    }
};

std::vector<std::string>
strsplit(std::string s, std::optional<std::string> delim = {})
{
    size_t pos = 0;
    std::vector<std::string> tokens;

    auto t_delim = delim.value_or(" ");

    while (true) {
        pos = s.find(t_delim);

        if (pos == std::string::npos) {
            tokens.push_back(s);
            break;
        }

        auto token = s.substr(0, pos);
        tokens.push_back(token);

        s.erase(0, pos + t_delim.length());
    }

    return tokens;
}

std::vector<uint64_t>
nums_to_bits(std::string& nums, uint64_t max_num, std::optional<std::string> delim = {})
{
    auto tokens = strsplit(nums, delim);

    uint64_t N = ceil(max_num / BIT_LENGTH);
    std::vector<uint64_t> bits(N, 0);

    for (auto& token : tokens) {
        auto n = std::stoull(token);
        auto ix = n / BIT_LENGTH;

        bits[ix] |= 1 << (n % BIT_LENGTH);
    }
}

std::string
bits_to_nums(std::vector<uint64_t>& bits, std::optional<std::string> delim = {})
{
    std::string nums = "";
    auto t_delim = delim.value_or("");

    auto num_to_str = [&](uint64_t bit, uint64_t offset) {
        while (bit != 0) {
            if ((bit & 1) != 0) {
                nums += fmt::format("{}{}", offset, t_delim);
            }
            offset += 1;
            bit >>= 1;
        }
    };

    uint64_t n = 0;
    for (auto& bit : bits) {
        uint64_t offset = n * BIT_LENGTH;
        num_to_str(bit, offset);
        n += 1;
    }

    return nums;
}

uint64_t
popcount64d(uint64_t num)
{
    uint64_t i = 0;
    while (i < num) {
        num &= num - 1;
        i += 1;
    }
    return i;
}

constexpr auto write_row =
  [](auto& file, std::vector<std::string>& row, std::string delim = ",") {
      int n = 0;
      for (auto& i : row) {
          if (n > 0) {
              file << delim;
          }
          file << i;
          n += 1;
      }
      file << "\n";
  };

int
main()
{
    using namespace std::filesystem;

    auto dir_path = path("keno/data/");
    auto wagers_path = path("wagers.csv");

    auto in_filepath = dir_path / wagers_path;
    auto out_filepath = dir_path / path("exploded_wagers.csv");

    io::CSVReader<6, io::trim_chars<' ', '\t'>, io::no_quote_escape<','>> in_file(
      in_filepath.string());

    std::ofstream out_file(out_filepath.string());

    int wager_id, begin_draw, end_draw;
    int qp;
    int ticket_cost, numbers_wagered_id;

    std::vector<std::string> header = {
        "wager_id",       "begin_draw",         "end_draw",       "qp",
        "ticket_cost",    "numbers_wagered_id", "draw_number_id", "numbers_matched",
        "low_match_mask", "high_match_mask",    "prize"
    };

    write_row(out_file, header);

    in_file.next_line();
    while (in_file.read_row(
      wager_id, begin_draw, end_draw, qp, ticket_cost, numbers_wagered_id)) {
        wager_row row{ .wager_id = wager_id,
                       .begin_draw = begin_draw,
                       .end_draw = end_draw,
                       .qp = qp,
                       .ticket_cost = ticket_cost,
                       .numbers_wagered_id = numbers_wagered_id };
    }
}