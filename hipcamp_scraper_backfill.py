from bs4 import BeautifulSoup
import requests
import pdb
import re
import json
import threading

def lowercase(string):
  return str(string).lower()

def snakecase(string):
    string = re.sub(r"[\-\.\s]", '_', str(string))
    if not string:
        return string
    return lowercase(string[0]) + re.sub(r"[A-Z]", lambda matched: '_' + lowercase(matched.group(0)), string[1:])

headers = {
  'X-Requested-With': 'XMLHttpRequest',
  'Referer': 'https://www.hipcamp.com/california/mare-island-preserve/bunker-bay-view-drive-in'
}
base_url = 'https://www.hipcamp.com'

campground_columns = ['picnic_table', 'weeknight_price_percentage', 'horseback_riding', 'seo_description', 'on_arrival', 'fishing', 'accessible_by', 'amenitiy_1', 'amenitiy_3', 'amenitiy_2', 'toilets', 'number_of_sites', 'title', 'r_v_hookup', 'short_overview', 'weeknight_discount', 'r_v_sanitation', 'o_h_v', 'went_active_at', 'elevation', 'cover_photo_id', 'short_name', 'tips_count', 'booking_window_in_months', 'default_accommodation', 'wildlife_watching', 'name', 'paddling', 'thumbnail_url', 'image_url', 'accepts_bookings', 'response_rate', 'state_name', 'display_title', 'save_count', 'call_me_maybe', 'wind_sports', 'asleep', 'full_name', 'lng', 'bookability', 'seo_title', 'is_instant_bookable', 'image_carousel_small_url', 'listing_referral_source_id', 'booking_url', 'deleted', 'park_url', 'park_slug', 'active', 'listing_type', 'slug', 'response_time', 'state_slug', 'inventory_outlet', 'favorites_count', 'whitewater_paddling', 'max_capacity', 'official_url', 'history', 'swimming', 'cost_per_additional_guest', 'hiking', 'overview', 'visible', 'import_source', 'faves', 'climbing', 'boating', 'accommodation', 'host_referral_source_id', 'amenities', 'count_of_recommendations', 'geo_boundary_multi', 'park_name', 'snow_sports', 'price_per_night', 'reservation_type', 'campground_id', 'minimum_nights', 'campground_url', 'check_out_before', 'state_id', 'biking', 'base_price', 'cancellation_policy', 'updated_at', 'showers', 'county_code', 'timezone', 'id', 'bookable', 'geo_center', 'top_level_host_referral_source_id', 'recommend_rate', 'base_capacity', 'check_in_after', 'trash', 'hipbook', 'standard_number_of_guests', 'recommends_percentage', 'additional_camper_fee_per_night', 'lat', 'verified', 'listing_referral_source_print', 'recommends_count', 'surfing', 'inventory_source', 'phone', 'host_description', 'created_at', 'wifi', 'geo_boundary', 'listing_referral_source_other']
review_columns = ['area_url', 'campground_id', 'already_upvoted', 'description_truncated', 'description', 'user_profile_url', 'created_at', 'user_first_name', 'campground_name', 'description_without_tags', 'user_avatar_url', 'formatted_description', 'location', 'upvotes_count', 'report_link', 'id', 'user_id', 'formatted_created_at', 'type', 'user_full_name', 'review_id']

def worker(start_value, end_value):
  for campground_id in range(start_value, end_value):
    try:
      campground_path = ''
      # try:
      campground_url = '/api/campgrounds/{}'.format(campground_id)

      request_to_get_campground_path = requests.get(base_url + campground_url, headers=headers)

      if request_to_get_campground_path.json().get('campground_url'):
        campground_path = request_to_get_campground_path.json()['campground_url']
        
        full_campground_html = requests.get(base_url + campground_path, headers=headers).text
        
        soup = BeautifulSoup(full_campground_html, "lxml")

        # only get active campsites
        if soup.find('div', {'class': 'booking-widget hipbook'}):
          print base_url + campground_path
          
          # get title
          title = soup.find("h1").text.strip()

          # get count of recommendations
          if soup.find('div', {'class': 'recommend-percentage'}):
            recommend_percentage = soup.find('div', {'class': 'recommend-percentage'}).text
            count_of_recommendations = soup.find('div', {'class': 'based-on'}).text
            recommend_rate = str(round(int([s.strip() for s in recommend_percentage.split('%')][0]) * .01, 2))
            count_of_recommendations = str([int(s) for s in count_of_recommendations.split(' ') if s.isdigit()][0])
          else:
            recommend_rate = None
            count_of_recommendations = None
          
          # get count of saves
          if soup.find('button', {'id': 'save-campground'}):
            if soup.find('button', {'id': 'save-campground'}).find('span', {'class': 'counter'}):
              save_count = soup.find('button', {'id': 'save-campground'}).find('span', {'class': 'counter'}).text
              if len(save_count.strip()) > 0:
                save_count = str(int(save_count))
              else:
                save_count = None

          # get base_price, standard_number_of_guests, and cost_per_additional_guest
          if soup.find('ul', {'class': 'summary'}):
            price_info = soup.find('ul', {'class': 'summary'}).find_all('li')
            if soup.find('span', {'data-guests': True}):
              standard_number_of_guests = str(int(soup.find('span', {'data-guests': True}).text))
            else:
              standard_number_of_guests = None

            if len(price_info) > 1:
              base_price = float(price_info[0].find('span', {'class': 'pull-right'}).text.split('$')[1])
              cost_per_additional_guest = float(price_info[1].find('span', {'class': 'pull-right'}).text.split('$')[1])
            else:
              base_price = float(price_info[0].find('span', {'class': 'pull-right'}).text.split('$')[1])
              cost_per_additional_guest = None

          # get verified status
          if soup.find('span', {'class': 'verified-status-tick'}):
            verified = True
          else:
            verified = False

          # get host description
          if soup.find('div', {'data-full-description': True}):
            host_description = soup.find('div', {'data-full-description': True}).text.strip()
          else:
            host_description = None

          # get core amenities
          if soup.find('div', {'class': 'core-amenities'}):
            final_amenities = {}
            amenities = [amenitiy.find('div', {'class': 'name'}).text for amenitiy in soup.find_all('div', {'class': 'core-amenity'})]
            for index, amenitiy in enumerate(amenities):
              final_amenities['amenitiy_{}'.format(index + 1)] = amenitiy.strip()
          else:
            amenities = {}

          # get details
          if soup.find('section', {'class': 'details'}):
            final_details = {}
            details = [li.text for li in soup.find('section', {'class': 'details'}).find_all('li')]
            for detail in details:
              detail.split(':')
              if snakecase(detail.split(':')[0].strip()) == 'no__of_sites':
                key = 'number_of_sites'
              else:
                key = snakecase(detail.split(':')[0].strip())
              value = detail.split(':')[1].strip()
              final_details[key] = value
          else:
            final_details = {}

          # get other features
          if soup.find('section', {'class': 'other-features'}):
            other_features = {}
            for feature in soup.find('section', {'class': 'other-features'}).find_all('div', {'class': 'feature'}):
              other_features[snakecase(feature.text.strip())] = True
          else:
            other_features = {}

          # get activites
          if soup.find('section', {'class': 'activities'}):
            activities = {}
            for activity in soup.find('section', {'class': 'activities'}).find_all('div', {'class': 'feature'}):
              activities[snakecase(activity.find('span', {'class': 'name'}).text.strip())] = True
          else:
            activities = {}

          # get elevation
          if soup.find('div', {'id': 'vibe-grid'}):
            if soup.find('div', {'id': 'vibe-grid'}).find_all('div', {'class': 'metric'}):
              elevation = str(int(''.join(re.findall('\d+', soup.find('div', {'id': 'vibe-grid'}).find_all('div', {'class': 'metric'})[1].find('big').text))))
            else:
              elevation = None
          else:
            elevation = None

          hipcamp_campground_data = json.loads(re.findall(r"\{.*\}", re.split('window\..* = ', soup.find_all('script')[-2].string)[1])[0])
          hipcamp_park_data = json.loads(re.findall(r"\{.*\}", re.split('window\..* = ', soup.find_all('script')[-2].string)[3])[0])
          
          campground_data = {
            'id': str(campground_id),
            'title': title,
            'recommend_rate': str(recommend_rate),
            'count_of_recommendations': str(count_of_recommendations),
            'save_count': str(save_count),
            'base_price': str(base_price),
            'cost_per_additional_guest': str(cost_per_additional_guest),
            'standard_number_of_guests': str(standard_number_of_guests),
            'verified': str(verified),
            'host_description': host_description,
            'elevation': str(elevation)
          }

          campground_data.update(final_details)
          campground_data.update(final_amenities)
          campground_data.update(other_features)
          campground_data.update(activities)
          campground_data.update(hipcamp_campground_data)
          campground_data.update(hipcamp_park_data)
          campground_data.pop("tip", None)
          campground_data['campground_id'] = campground_id

          # write campground data to campaground file
          with open('./campgrounds.csv', 'a') as f:
            campground_fields = []
            for column in campground_columns:
              val = campground_data.get(column, '')
              if type(val).__name__ == 'str' or type(val).__name__ == 'unicode':
                if column == 'overview' or column == 'history':
                  val = ' '.join([el.text for el in BeautifulSoup(val, 'lxml').find_all('p')]).encode('utf-8', 'ignore').decode('utf-8')
                else:
                  val = val.encode('utf-8', 'ignore').decode('utf-8')
              else:
                val = str(val)
              campground_fields.append(re.sub(r"\s+", " ", ' '.join(val.split('\n'))))
            f.write(' | '.join(campground_fields).encode('utf8') + '\n')

          reviews = requests.get(base_url + "/api/campgrounds/{}/tips".format(campground_id), headers=headers).json()
          for review in reviews['tips']:
            review['campground_id'] = campground_id
            review['review_id'] = review['id']
            with open('reviews.csv', 'a') as f:
              review_fields = []
              for column in review_columns:
                val = review.get(column, '')
                if type(val).__name__ == 'str' or type(val).__name__ == 'unicode':
                  if column == 'formatted_description':
                    val = ' '.join([el.text for el in BeautifulSoup(val, 'lxml').find_all('p')]).encode('utf-8', 'ignore').decode('utf-8')
                  else:
                    val = val.encode('utf-8', 'ignore').decode('utf-8')
                else:
                  val = str(val)
                review_fields.append(re.sub(r"\s+", " ", ' '.join(val.split('\n'))))
              f.write(' | '.join(review_fields).encode('utf8') + '\n')

          print '{} - successful'.format(campground_id)
          with open('log.txt', 'a') as f:
            f.write('{} - successful\n'.format(campground_id))
        else:
          campground_data = {}
          hipcamp_campground_data = json.loads(re.findall(r"\{.*\}", re.split('window\..* = ', soup.find_all('script')[-2].string)[1])[0])
          hipcamp_park_data = json.loads(re.findall(r"\{.*\}", re.split('window\..* = ', soup.find_all('script')[-2].string)[3])[0])

          campground_data.update(hipcamp_campground_data)
          campground_data.update(hipcamp_park_data)

          with open('./campgrounds.csv', 'a') as f:
            campground_fields = []
            for column in campground_columns:
              val = campground_data.get(column, '')
              if type(val).__name__ == 'str' or type(val).__name__ == 'unicode':
                if column == 'overview' or column == 'history':
                  val = ' '.join([el.text for el in BeautifulSoup(val, 'lxml').find_all('p')]).encode('utf-8', 'ignore').decode('utf-8')
                else:
                  val = val.encode('utf-8', 'ignore').decode('utf-8')
              else:
                val = str(val)
              campground_fields.append(re.sub(r"\s+", " ", ' '.join(val.split('\n'))))
            f.write(' | '.join(campground_fields).encode('utf8') + '\n')

          reviews = requests.get(base_url + "/api/campgrounds/{}/tips".format(campground_id), headers=headers).json()
          for review in reviews['tips']:
            review['campground_id'] = campground_id
            review['review_id'] = review['id']
            with open('reviews.csv', 'a') as f:
              review_fields = []
              for column in review_columns:
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
              f.write(' | '.join(review_fields).encode('utf8') + '\n')

          # write inactive to file
          print '{} - inactive but fetched some data'.format(base_url + campground_path)
          with open('log.txt', 'a') as f:
            f.write('{} - inactive but fetched some data\n'.format(base_url + campground_path))
      else:
        # write id not found to file
        print '{} - id not found'.format(campground_id)
        with open('log.txt', 'a') as f:
          f.write('{} - id not found\n'.format(campground_id))
    except Exception as e:
      print e
      print e.__doc__
      with open('log.txt', 'a') as f:
        f.write('!!!!!!!' + e.__doc__ + '!!!!!!!\n')
      print 'error!'
      next

worker(14117, 16000)
