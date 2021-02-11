#include "csv.h"
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "fmt/format.h"

using namespace std::literals::string_literals;

struct wager_row
{
    int wager_id, begin_draw, end_draw;
    int qp;
    int ticket_cost, numbers_wagered_id, draw_number_id;
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

        t_row.draw_number_id = draw_number;

        rows.push_back(t_row);
    }

    return rows;
};

constexpr auto write_row = [](auto& file, wager_row& row) {
    auto s = fmt::format("{},{},{},{},{},{},{}\n",
                         row.wager_id,
                         row.begin_draw,
                         row.end_draw,
                         row.qp,
                         row.ticket_cost,
                         row.numbers_wagered_id,
                         row.draw_number_id);
    file << s;
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

    out_file
      << R"("wager_id","begin_draw","end_draw","qp","ticket_cost","numbers_wagered_id","draw_number_id"\n)";

    in_file.next_line();
    while (in_file.read_row(
      wager_id, begin_draw, end_draw, qp, ticket_cost, numbers_wagered_id)) {
        wager_row base_row{ .wager_id = wager_id,
                            .begin_draw = begin_draw,
                            .end_draw = end_draw,
                            .qp = qp,
                            .ticket_cost = ticket_cost,
                            .numbers_wagered_id = numbers_wagered_id };

        auto rows = explode_row(base_row);

        for (auto& row : rows) {
            write_row(out_file, row);
        }
    }
}