#include "csv.h"
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "fmt/format.h"

using namespace std::literals::string_literals;

struct wager_row
{
    int id;
    int begin_draw, end_draw;
    bool qp;
    int ticket_cost;
    std::string numbers_wagered;
};

constexpr auto explode_row = [](wager_row& row) {
    auto begin_draw = row.begin_draw;
    auto end_draw = row.end_draw;

    auto draw_delta = (end_draw - begin_draw) + 1;

    std::vector<wager_row> rows;
    rows.reserve(draw_delta + 1);

    for (auto i = 0; i < draw_delta; i++) {
        auto t_row = row;
        auto draw_number = begin_draw + i;

        t_row.begin_draw = draw_number;
        t_row.end_draw = draw_number;

        rows.push_back(t_row);
    }

    return rows;
};

constexpr auto write_row = [](auto& file, wager_row& row) {
    auto s = fmt::format("{},{},{},{},{},{}\n",
                         row.id,
                         row.begin_draw,
                         row.end_draw,
                         row.qp,
                         row.ticket_cost,
                         row.numbers_wagered);
    file << s;
};

int
main()
{
    using namespace std::filesystem;

    auto dir_path = path("keno/data/keno_2017_2019/");
    auto wagers_path = path("keno_wager_data.csv");

    auto in_filepath = dir_path / wagers_path;
    auto out_filepath = dir_path / path("tmp.csv");

    io::CSVReader<5, io::trim_chars<' ', '\t'>, io::no_quote_escape<';'>> in_file(
      in_filepath.string());

    std::ofstream out_file(out_filepath.string());

    int begin_draw, end_draw;
    std::string qp;
    int ticket_cost;
    std::string numbers_wagered;

    int n = 0;

    in_file.next_line();
    while (in_file.read_row(begin_draw, end_draw, qp, ticket_cost, numbers_wagered)) {
        wager_row base_row{ .id = n,
                            .begin_draw = begin_draw,
                            .end_draw = end_draw,
                            .qp = qp == "T" ? true : false,
                            .ticket_cost = ticket_cost,
                            .numbers_wagered = numbers_wagered };

        auto rows = explode_row(base_row);

        for (auto& row : rows) {
            write_row(out_file, row);
        }

        n += 1;
    }
}