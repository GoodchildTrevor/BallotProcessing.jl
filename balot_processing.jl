@time begin

using DataFrames
using XLSX
using OrderedCollections

# --- Constants ---
const FILE_PATH = "1933.xlsx"

# --- List of nomination categories ---
const NOMINATIONS = [
    "director",
    "actor",
    "actress",
    "actor2",
    "actress2",
    "original_screenplay",
    "adapted_screenplay",
    "operator",
    "editing",
    "soundtrack",
    "song",
    "art_direction",
    "costumes",
    "make_up",
    "effects",
    "sound",
    "stunts",
    # "animation",
    "documentation",
    "russian",
    "live_action_short",
    "animated_short",
    "documentary_short",
    "debut",
    "ensemble",
    "using_music",
    "young_actor",
    "young_actress",
    "choreography",
    "special_mentions"
]

"""
    delete_non_breaking_spaces(text)

Replace non-breaking spaces (U+00A0) with regular spaces and strip leading/trailing whitespace.
If input is not a string, convert it to one.

Parameters
----------
text : Any
    Input value to clean.

Returns
-------
String
    Cleaned string.
"""
function delete_non_breaking_spaces(text)
    if text isa String
        return replace(text, "\u00A0" => " ") |> strip
    else
        return string(text)
    end
end

"""
    postfix(score)

Return a Russian-language string with correct grammatical form for score (e.g., "1 балл", "2 балла", "5 баллов").

Parameters
----------
score : Int
    Numerical score.

Returns
-------
String
    Formatted string with correct postfix.
"""
function postfix(score::Int)
    if score == 1
        return "$score балл"
    elseif score > 4
        return "$score баллов"
    else
        return "$score балла"
    end
end

"""
    write_down_excel(sheet, name, score, mentions, row, column)

Write a row of data (name, score, mentions) into an Excel worksheet starting at given cell.

Parameters
----------
sheet : XLSX.Worksheet
    Target worksheet.
name : String
    Name of nominee or film.
score : Any
    Score value (will be converted to string).
mentions : String
    Comma-separated list of mentions.
row : Int
    Starting row index.
column : Int
    Starting column index.
"""
function write_down_excel(sheet, name, score, mentions, row, column)
    XLSX.setdata!(sheet, XLSX.CellRef(row, column), String(name))
    XLSX.setdata!(sheet, XLSX.CellRef(row, column + 1), string(score))
    XLSX.setdata!(sheet, XLSX.CellRef(row, column + 2), string(mentions))
end

"""
    process_coincidences(nominations_dict)

Find individuals who appear in both main and supporting acting categories (actor/actress + actor2/actress2)
and compute combined scores.

Parameters
----------
nominations_dict : Dict{String, Dict}
    Dictionary mapping nomination categories to nominee dictionaries.

Returns
-------
Vector{Tuple}
    Each tuple: (name, total_score, main_score, support_score, mentions, category)
"""
function process_coincidences(nominations_dict)
    main_categories = ["actor", "actress"]
    support_categories = ["actor2", "actress2"]

    coincidences = []

    for i in 1:length(main_categories)
        main_nom = main_categories[i]
        support_nom = support_categories[i]

        data_main = get(nominations_dict, main_nom, Dict())
        data_support = get(nominations_dict, support_nom, Dict())

        for (key, values_main) in data_main
            if haskey(data_support, key)
                score_main = values_main["score"]
                score_support = data_support[key]["score"]
                total_score = score_main + score_support
                combined_mentions = vcat(values_main["mentions"], data_support[key]["mentions"])
                push!(coincidences, (
                    key,
                    total_score,
                    score_main,
                    score_support,
                    combined_mentions,
                    main_nom
                ))
            end
        end
    end

    return coincidences
end

"""
    process_data(df::DataFrame, nominees::DataFrame, nomination::Vector{String})

Process voting data to compute scores for films and nominees.

Parameters
----------
df : DataFrame
    Raw voting data (rows = voters, columns = rankings).
nominees : DataFrame
    List of nominees per category.
nomination : Vector{String}
    List of nomination category names.

Returns
-------
Tuple{Dict, Dict}
    - movies_dict: scores and mentions for top 10 films.
    - nominations_dict: scores and mentions per nominee per category.
"""
function process_data(df::DataFrame, nominees::DataFrame, nomination::Vector{String})
    df = mapcols(x -> coalesce.(x, "xxx"), df)
    df_transpose = permutedims(df, 1)
    df_transpose = select!(df_transpose, Not([:"Ваш ник на Форуме Кинопоиска:"]))
    df_movies = df_transpose[1:10, :]
    df_other = df_transpose[11:end, :]

    # Count film scores (top 10 positions)
    movies_dict = Dict()
    for (name, values) in pairs(eachcol(df_movies))
        for position in 1:10
            movie_key = string(values[position])
            if haskey(movies_dict, movie_key)
                movies_dict[movie_key]["score"] += 11 - position
                push!(movies_dict[movie_key]["mentions"], (name, postfix(11 - position)))
            else
                movies_dict[movie_key] = Dict(
                    "score" => 11 - position,
                    "mentions" => [(name, postfix(11 - position))]
                )
            end
        end
    end

    # Count nomination scores
    nominations_dict = Dict()
    for nom in 1:length(nomination)
        nominee_col = nominees[:, nomination[nom]]
        nominations_dict[nomination[nom]] = Dict()
        for a in 1:length(nominee_col)
            clean_nominee = delete_non_breaking_spaces(nominee_col[a])
            for (name, values) in pairs(eachcol(df_other))
                if occursin(clean_nominee, string(values[nom]))
                    if haskey(nominations_dict[nomination[nom]], clean_nominee)
                        nominations_dict[nomination[nom]][clean_nominee]["score"] += 1
                        push!(nominations_dict[nomination[nom]][clean_nominee]["mentions"], name)
                    else
                        nominations_dict[nomination[nom]][clean_nominee] = Dict("score" => 1, "mentions" => [name])
                    end
                end
            end
        end
    end

    return movies_dict, nominations_dict
end

# --- Load data ---
data_original, headers_original = XLSX.readtable(FILE_PATH, "номинанты")
df_original = DataFrame(data_original, Symbol.(headers_original))

data_nominees, headers_nominees = XLSX.readtable(FILE_PATH, "списки")
nominees = DataFrame(data_nominees, Symbol.(headers_nominees))

# --- Rename nominee columns to match nomination categories ---
rename!(nominees, names(nominees) .=> NOMINATIONS)

# --- Auto-generate voting slices (multiples of 10) ---
n_votes = nrow(df_original)
max_slice = (n_votes ÷ 10) * 10
slices = max_slice ≥ 10 ? collect(10:10:max_slice) : Int[]

# --- Final results ---
final_movies_dict, final_nominations_dict = process_data(df_original, nominees, NOMINATIONS)

# --- Prepare sheet names and results ---
winner_sheet_names = ["победители"]
if !isempty(slices)
    winner_sheet_names = vcat(winner_sheet_names, ["победители $s" for s in slices])
end
sheet_names = vcat(winner_sheet_names, ["совпадения"])

# --- Build all_results: [final, slice1, slice2, ..., coincidences] ---
all_results = Tuple{Union{Dict, Nothing}, Dict}[]

push!(all_results, (final_movies_dict, final_nominations_dict))

for slice in slices
    subset_df = df_original[1:slice, :]
    movies_dict, nominations_dict = process_data(subset_df, nominees, NOMINATIONS)
    push!(all_results, (movies_dict, nominations_dict))
end

push!(all_results, (nothing, final_nominations_dict))  # for "совпадения"

# --- Write results to Excel ---
XLSX.openxlsx(FILE_PATH, mode="rw") do xf
    sheets = Dict{String, XLSX.Worksheet}()

    # Create or clear sheets
    for sn in sheet_names
        if sn in XLSX.sheetnames(xf)
            sheet = xf[sn]
            # Clear sheet (up to reasonable size)
            for row in 1:1000, col in 1:100
                XLSX.setdata!(sheet, XLSX.CellRef(row, col), "")
            end
            sheets[sn] = sheet
        else
            sheets[sn] = XLSX.addsheet!(xf, sn)
        end
    end

    # Write data to each sheet
    for (sn, data) in zip(sheet_names, all_results)
        winners = sheets[sn]

        if sn == "совпадения"
            coincidences = process_coincidences(data[2])
            # Headers
            XLSX.setdata!(winners, XLSX.CellRef(1, 1), "Имя")
            XLSX.setdata!(winners, XLSX.CellRef(1, 2), "Баллы (итого)")
            XLSX.setdata!(winners, XLSX.CellRef(1, 3), "Первый план")
            XLSX.setdata!(winners, XLSX.CellRef(1, 4), "Второй план")
            XLSX.setdata!(winners, XLSX.CellRef(1, 5), "Упоминания")

            for (i, (name, total_score, score_main, score_support, mentions, _)) in enumerate(coincidences)
                XLSX.setdata!(winners, XLSX.CellRef(i+1, 1), String(name))
                XLSX.setdata!(winners, XLSX.CellRef(i+1, 2), string(total_score))
                XLSX.setdata!(winners, XLSX.CellRef(i+1, 3), string(score_main))
                XLSX.setdata!(winners, XLSX.CellRef(i+1, 4), string(score_support))
                XLSX.setdata!(winners, XLSX.CellRef(i+1, 5), join(mentions, ", "))
            end
        else
            movies_dict, nominations_dict = data

            # Best films
            sorted_movies_dict = OrderedDict(sort(collect(movies_dict), by = x -> x[2]["score"], rev = true))
            delete!(sorted_movies_dict, "xxx")

            for (x, (keys, info)) in enumerate(pairs(sorted_movies_dict))
                write_down_excel(winners, keys, info["score"], join(info["mentions"], ", "), x+1, 1)
            end

            # Per-nomination results
            nomination_plus = ["movie"; NOMINATIONS]
            for z in 1:length(nomination_plus)
                write_down_excel(
                    winners,
                    nomination_plus[z],
                    "points_$(nomination_plus[z])",
                    "mentions_by_$(nomination_plus[z])",
                    1,
                    (z*3)-2
                )
            end

            for (col, nom) in enumerate(NOMINATIONS)
                sorted_data = OrderedDict(
                    sort(collect(nominations_dict[nom]), by = x -> x[2]["score"], rev = true)
                )
                for (line, (keys, info)) in enumerate(pairs(sorted_data))
                    write_down_excel(
                        winners, keys, info["score"], join(info["mentions"], ", "), line+1, 4+((col-1)*3)
                    )
                end
            end
        end
    end
end

println("✅ All results written to file $FILE_PATH")

end
