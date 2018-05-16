require 'pry'
require 'rest-client'
require 'json'

client_id = '587748278082312'
client_secret = '653f038ce5648e38a231609066407d86'
response = RestClient.post 'https://graph.facebook.com/v2.6/oauth/access_token', :client_id=>client_id, :client_secret=>client_secret,:grant_type=>'client_credentials'
# response = RestClient.post 'https://graph.facebook.com/v2.6/oauth/access_token?client_id=587748278082312&client_secret=653f038ce5648e38a231609066407d86&grant_type=client_credentials', :some => 'none'
token = JSON.parse(response)["access_token"]
puts "The extracted token is:" + token + "\n\n\n"
data = RestClient.get('https://graph.facebook.com/v2.8/thegarden/events', {params: {access_token: token, debug: 'all', format: 'json', method: 'get', pretty: '0', suppress_http_code: '1'}})
puts "This is the result from the query: \n" + data
