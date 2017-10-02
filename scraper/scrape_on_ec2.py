from bs4 import BeautifulSoup
import requests
import re
import json
import threading
import numpy as np

def lowercase(string):
  return str(string).lower()

def snakecase(string):
  string = re.sub(r"[\-\.\s]", '_', str(string))
  if not string:
      return string
  return lowercase(string[0]) + re.sub(r"[A-Z]", lambda matched: '_' + lowercase(matched.group(0)), string[1:])

class HipCampScraper(object):
  
  REQUEST_HEADERS    = {'X-Requested-With': 'XMLHttpRequest', 'Referer': 'https://www.hipcamp.com/california/mare-island-preserve/bunker-bay-view-drive-in'}
  BASE_URL           = 'https://www.hipcamp.com'
  CAMPGROUND_COLUMNS = ['picnic_table', 'weeknight_price_percentage', 'horseback_riding', 'seo_description', 'on_arrival', 'fishing', 'accessible_by', 'amenitiy_1', 'amenitiy_3', 'amenitiy_2', 'toilets', 'number_of_sites', 'title', 'r_v_hookup', 'short_overview', 'weeknight_discount', 'r_v_sanitation', 'o_h_v', 'went_active_at', 'elevation', 'cover_photo_id', 'short_name', 'tips_count', 'booking_window_in_months', 'default_accommodation', 'wildlife_watching', 'name', 'paddling', 'thumbnail_url', 'image_url', 'accepts_bookings', 'response_rate', 'state_name', 'display_title', 'save_count', 'call_me_maybe', 'wind_sports', 'asleep', 'full_name', 'lng', 'bookability', 'seo_title', 'is_instant_bookable', 'image_carousel_small_url', 'listing_referral_source_id', 'booking_url', 'deleted', 'park_url', 'park_slug', 'active', 'listing_type', 'slug', 'response_time', 'state_slug', 'inventory_outlet', 'favorites_count', 'whitewater_paddling', 'max_capacity', 'official_url', 'history', 'swimming', 'cost_per_additional_guest', 'hiking', 'overview', 'visible', 'import_source', 'faves', 'climbing', 'boating', 'accommodation', 'host_referral_source_id', 'amenities', 'count_of_recommendations', 'geo_boundary_multi', 'park_name', 'snow_sports', 'price_per_night', 'reservation_type', 'campground_id', 'minimum_nights', 'campground_url', 'check_out_before', 'state_id', 'biking', 'base_price', 'cancellation_policy', 'updated_at', 'showers', 'county_code', 'timezone', 'id', 'bookable', 'geo_center', 'top_level_host_referral_source_id', 'recommend_rate', 'base_capacity', 'check_in_after', 'trash', 'hipbook', 'standard_number_of_guests', 'recommends_percentage', 'additional_camper_fee_per_night', 'lat', 'verified', 'listing_referral_source_print', 'recommends_count', 'surfing', 'inventory_source', 'phone', 'host_description', 'created_at', 'wifi', 'geo_boundary', 'listing_referral_source_other']
  REVIEW_COLUMNS     = ['area_url', 'campground_id', 'already_upvoted', 'description_truncated', 'description', 'user_profile_url', 'created_at', 'user_first_name', 'campground_name', 'description_without_tags', 'user_avatar_url', 'formatted_description', 'location', 'upvotes_count', 'report_link', 'id', 'user_id', 'formatted_created_at', 'type', 'user_full_name', 'review_id']

  def __init__(self, campground_ids_to_scrape=range(1, 80000), write_mode=False, campgrounds_file_name='./campgrounds_2.csv', reviews_file_name='./reviews_2.csv', log_file_name='./scrape.log'):
    print('write mode ON' if write_mode else 'write mode OFF')

    self.campgrounds_file_name    = campgrounds_file_name
    self.reviews_file_name        = reviews_file_name
    self.log_file_name            = log_file_name
    self.campground_ids_to_scrape = campground_ids_to_scrape
    self.write_mode               = write_mode
    self._scrape()

  def _scrape(self):
    for campground_id in self.campground_ids_to_scrape:
      try:
        campground_path = ''
        campground_url = '/api/campgrounds/{}'.format(campground_id)
        request_to_get_campground_path = requests.get(self.BASE_URL + campground_url, headers=self.REQUEST_HEADERS)
        campground_path = request_to_get_campground_path.json().get('campground_url', None)

        if campground_path:
          try:
            self._scrape_campground(campground_id, campground_path)
          except:
            self._log('{}, failed, fetch or write campground data'.format(campground_id))
            continue
          
          try:
            self._scrape_reviews(campground_id)
          except:
            self._log('{}, failed, fetch or write reviews data'.format(campground_id))
            continue

          self._log('{0}, success, fetch and write campground and reviews'.format(campground_id))
        else:
          self._log('{}, not found, campground id not found'.format(campground_id))

      except:
        self._log('{}, failed, initial campground fetch'.format(campground_id))
        continue
      
  def _scrape_reviews(self, campground_id):
    reviews = requests.get(self.BASE_URL + "/api/campgrounds/{}/tips".format(campground_id), headers=self.REQUEST_HEADERS).json()
    self._write_review_data(reviews, campground_id)
          
  def _scrape_campground(self, campground_id, campground_path):
    full_campground_html = requests.get(self.BASE_URL + campground_path, headers=self.REQUEST_HEADERS).text
    
    soup = BeautifulSoup(full_campground_html, "lxml")

    final_campground_data                               = {}
    final_campground_data['campground_id']              = str(campground_id)
    final_campground_data['title']                      = self._find_title(soup)
    final_campground_data['recommend_rate']             = self._find_recommend_rate(soup)
    final_campground_data['count_of_recommendations']   = self._find_count_of_recommendations(soup)
    final_campground_data['save_count']                 = self._find_save_count(soup)
    final_campground_data['base_price']                 = self._find_base_price(soup)
    final_campground_data['standard_number_of_guests']  = self._find_standard_number_of_guests(soup)
    final_campground_data['cost_per_additional_guest']  = self._find_cost_per_additional_guest(soup)
    final_campground_data['is_verified']                = self._find_verified_status(soup)
    final_campground_data['host_description']           = self._find_host_description(soup)
    final_campground_data['elevation']                  = self._find_elevation(soup)

    amenities       = self._find_amenities(soup)
    details         = self._find_details(soup)
    other_features  = self._find_other_features(soup)
    activities      = self._find_activities(soup)

    hipcamp_campground_data_off_window  = self._fetch_campground_data_off_window(soup)
    hipcamp_park_data_off_window        = self._fetch_park_data_off_window(soup)

    final_campground_data.update(amenities)
    final_campground_data.update(details)
    final_campground_data.update(other_features)
    final_campground_data.update(activities)
    final_campground_data.update(hipcamp_campground_data_off_window)
    final_campground_data.update(hipcamp_park_data_off_window)
    final_campground_data.pop("tip", None)
    
    self._write_campground_data(final_campground_data)

  def _write_campground_data(self, final_campground_data):
    campground_fields = []
    for column in self.CAMPGROUND_COLUMNS:
      val = final_campground_data.get(column, '')
      if type(val).__name__ == 'str' or type(val).__name__ == 'unicode':
        if column == 'overview' or column == 'history':
          val = ' '.join([el.text for el in BeautifulSoup(val, 'lxml').find_all('p')]).encode('utf-8', 'ignore').decode('utf-8')
        else:
          val = val.encode('utf-8', 'ignore').decode('utf-8')
      else:
        val = str(val)
      campground_fields.append(re.sub(r"\s+", " ", ' '.join(val.split('\n'))))
    output = ' | '.join(campground_fields).encode('utf8') + '\n'
    if self.write_mode:
      with open(self.campgrounds_file_name, 'a') as f:
        f.write(output)
    else:
      print output

  def _log(self, message):
    with open(self.log_file_name, 'a') as f:
      f.write(message + '\n')

  def _write_review_data(self, reviews, campground_id):
    if reviews.get('tips', None):
      for review in reviews['tips']:
        review['campground_id'] = campground_id
        review['review_id']     = review['id']
        
        review_fields = []
        for column in self.REVIEW_COLUMNS:
          val = review.get(column, '')
          if type(val).__name__ == 'str' or type(val).__name__ == 'unicode':
            if column == 'formatted_description':
              val = ' '.join([el.text for el in BeautifulSoup(val, 'lxml').find_all('p')]).encode('utf-8', 'ignore').decode('utf-8')
            elif column == 'description':
              val = ' '.join(val.split('\n'))
            else:
              val = val.encode('utf-8', 'ignore').decode('utf-8')
          else:
            val = str(val)
          review_fields.append(re.sub(r"\s+", " ", ' '.join(val.split('\n'))))
        output = ' | '.join(review_fields).encode('utf8') + '\n'
        if self.write_mode:
          with open(self.reviews_file_name, 'a') as f:
            f.write(output)
        else:
          print output

  def _fetch_park_data_off_window(self, soup):
    try:
      park_data = json.loads(re.findall(r"\{.*\}", re.split('window\..* = ', soup.find_all('script')[-2].string)[3])[0])
    except:
      park_data = {}
    return park_data

  def _fetch_campground_data_off_window(self, soup):
    try:
      campground_data = json.loads(re.findall(r"\{.*\}", re.split('window\..* = ', soup.find_all('script')[-2].string)[1])[0])
    except:
      campground_data = {}
    return campground_data

  def _find_elevation(self, soup):
    try:
      elevation = ''.join(re.findall('\d+', soup.find('div', {'id': 'vibe-grid'}).find_all('div', {'class': 'metric'})[1].find('big').text))
    except:
      elevation = ''
    return elevation

  def _find_activities(self, soup):
    activities = {}
    try:
      for activity in soup.find('section', {'class': 'activities'}).find_all('div', {'class': 'feature'}):
        activities[snakecase(activity.find('span', {'class': 'name'}).text.strip())] = True
    except:
      activities = {}
    return activities

  def _find_other_features(self, soup):
    other_features = {}
    try:
      for feature in soup.find('section', {'class': 'other-features'}).find_all('div', {'class': 'feature'}):
        other_features[snakecase(feature.text.strip())] = True
    except:
      other_features = {}
    return other_features

  def _find_details(self, soup):
    # returns a dictionary of details
    # eg: {'listing_type': 'private', 'no_of_sites': '2'}
    details = {}
    try:
      raw_details = [li.text for li in soup.find('section', {'class': 'details'}).find_all('li')]
      for detail in raw_details:
        if snakecase(detail.split(':')[0].strip()) == 'no__of_sites':
          key = 'number_of_sites'
        else:
          key = snakecase(detail.split(':')[0].strip())
        value = detail.split(':')[1].strip()
        details[key] = value
    except:
      details = {}
    return details
  
  def _find_amenities(self, soup):
    # returns a dictionary of generically named amenities
    # eg: {'amenity': 'trash', 'amenity': 'toilet'}
    amenities = {}
    try:
      raw_amenities = [amenity.find('div', {'class': 'name'}).text for amenity in soup.find_all('div', {'class': 'core-amenity'})]
      for index, amenitiy in enumerate(raw_amenities):
        amenities['amenitiy_{}'.format(index + 1)] = amenitiy.strip()
    except:
      amenities = {}
    return amenities    

  def _find_host_description(self, soup):
    try:
      host_description = soup.find('div', {'data-full-description': True}).text.strip()
    except:
      host_description = ''
    return host_description

  def _find_verified_status(self, soup):
    try:
      is_verified = True if soup.find('span', {'class': 'verified-status-tick'}) else False
    except:
      is_verified = False
    return is_verified
  
  def _find_cost_per_additional_guest(self, soup):
    try:
      price_info = soup.find('ul', {'class': 'summary'}).find_all('li')
      cost_per_additional_guest = price_info[1].find('span', {'class': 'pull-right'}).text.split('$')[1]
    except:
      cost_per_additional_guest = ''
    return cost_per_additional_guest

  def _find_base_price(self, soup):
    try:
      price_info = soup.find('ul', {'class': 'summary'}).find_all('li')
      base_price = price_info[0].find('span', {'class': 'pull-right'}).text.split('$')[1]
    except:
      base_price = ''
    return base_price
        

  def _find_standard_number_of_guests(self, soup):
    try:
      standard_number_of_guests = soup.find('span', {'data-guests': True}).text
    except:
      standard_number_of_guests = ''
    return standard_number_of_guests

  def _find_save_count(self, soup):
    try:
      save_count = soup.find('button', {'id': 'save-campground'}).find('span', {'class': 'counter'}).text
    except:
      save_count = ''
    return save_count

  def _find_count_of_recommendations(self, soup):
    try:
      raw_count_of_recommendations = soup.find('div', {'class': 'based-on'}).text
      count_of_recommendations = str([int(s) for s in raw_count_of_recommendations.split(' ') if s.isdigit()][0])
    except:
      count_of_recommendations = ''
    return count_of_recommendations

  def _find_title(self, soup):
    try:
      title = soup.find("h1").text.strip()
    except:
      title = ''
    return title

  def _find_recommend_rate(self, soup):
    try:
      recommend_percentage = soup.find('div', {'class': 'recommend-percentage'}).text
      recommend_rate = str(round(int([s.strip() for s in recommend_percentage.split('%')][0]) * .01, 2))
    except:
      recommend_rate = ''
    return recommend_rate 

threads = []
start_id, end_id, thread_count = [1, 20000, 40]
campground_ids_for_threads_to_scrape = np.array_split(range(start_id, end_id), thread_count)
for ids in campground_ids_for_threads_to_scrape:
    t = threading.Thread(target=HipCampScraper, kwargs={'campground_ids_to_scrape': ids, 'write_mode': True})
    threads.append(t)
    t.start()
