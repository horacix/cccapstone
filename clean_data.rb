require 'fastest-csv'

dir = '*.csv'
if !ARGV[0].nil?
  dir = "*#{ARGV[0]}.csv"
end
total = Dir.glob(dir).count
Dir.glob(dir).each_with_index do |file, index|
  STDERR.puts "#{ARGV[0]}: #{index}/#{total}"
  line = 0
  FastestCSV.foreach(file) do |row|
    clean_array = [row[4], row[5], row[6], row[10], row[11], row[17], row[23], row[25], row[34], row[36]]
    clean_array.map! {|d| !d.nil? && d.gsub('"','') }
    puts clean_array.join(',') unless line == 0
    line = 1
  end
end
