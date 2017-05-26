﻿using System;
using System.Collections.Generic;
using System.Linq;
using Sitecore.Analytics.Model;
using Sitecore.Cintel.Commons;
using Sitecore.Cintel.Configuration;
using Sitecore.Cintel.Search;
using Sitecore.ContentSearch;
using Sitecore.ContentSearch.Analytics.Models;
using Sitecore.ContentSearch.Linq;
using Sitecore.Cintel;

namespace Sitecore.Support.Cintel
{
  public class ContactSearchProvider : IContactSearchProvider
  {
    public ResultSet<List<IContactSearchResult>> Find(ContactSearchParameters parameters)
    {
      var finalResultSet = new ResultSet<List<IContactSearchResult>>(parameters.PageNumber, parameters.PageSize);
      var analyticsIndex = ContentSearchManager.GetIndex(CustomerIntelligenceConfig.ContactSearch.SearchIndexName);

      using (var ctx = analyticsIndex.CreateSearchContext())
      {
        var rawSearchResults = QueryIndex(ctx, parameters);
        var contactSearchResults = rawSearchResults.Hits.Select(h => h.Document).ToList();
        finalResultSet.TotalResultCount = rawSearchResults.TotalSearchResults;

        var contacts = contactSearchResults.Select(sr =>
          {
            IContactSearchResult contact = BuildBaseResult(sr);

            var visit = ctx.GetQueryable<IndexedVisit>()
                           .Where(iv => iv.ContactId == contact.ContactId)
                           .OrderByDescending(iv => iv.StartDateTime)
                           .Take(1)
                           .FirstOrDefault();

            if (null != visit)
            {
              PopulateLatestVisit(visit, ref contact);
            }

            return contact;
          }).OrderBy(c => c.FirstName).ThenBy(c => c.LatestVisitStartDateTime).ToList();

        finalResultSet.Data.Dataset.Add("ContactSearchResults", contacts);
      }

      return finalResultSet;

    }

    #region private methods

    private SearchResults<IndexedContact> QueryIndex(IProviderSearchContext ctx, ContactSearchParameters parameters)
    {
      //boost example:   //only works on direct match
      //var contactResults = queryable.Where(q => q.IdentificationInfo.Contains(text) || q.FirstName == text.Boost(5) || q.Surname == text.Boost(4)).Page(parameters.PageNumber-1, parameters.PageSize).GetResults();

      var queryable = ctx.GetQueryable<IndexedContact>();
      string text = parameters.Match;

      if (string.IsNullOrEmpty(text.Trim()) || text == "*")
      {
        return queryable.Page(parameters.PageNumber - 1, parameters.PageSize).GetResults();
      }
      
      string wildcard = string.Concat("*", text, "*");
      int slop = 10;

      var results =
        queryable.Where(q => (q.FullName.MatchWildcard(wildcard) || q.Emails.MatchWildcard(wildcard)));

      if (!results.Any())
      {
        results = queryable.Where(q => q.FullName.Like(text, slop) || q.Emails.Like(text, slop));        
      }

      return results.Page(parameters.PageNumber - 1, parameters.PageSize).GetResults();
    }

    private IContactSearchResult BuildBaseResult(IndexedContact indexedContact)
    {
      ContactIdentificationLevel ident;
      if (! Enum.TryParse(indexedContact.IdentificationLevel, true, out ident))
      {
        ident = ContactIdentificationLevel.None;
      }

      var contact = new ContactSearchResult
        {
          IdentificationLevel = (int)ident,
          ContactId = indexedContact.ContactId,
          FirstName = indexedContact.FirstName,
          MiddleName = indexedContact.MiddleName,
          Surname = indexedContact.Surname,
          PreferredEmail = indexedContact.PreferredEmail,
          JobTitle = indexedContact.JobTitle,
          Value = indexedContact.Value,
          VisitCount = indexedContact.VisitCount
        };

      return contact;
    }

    private void PopulateLatestVisit(IndexedVisit visit, ref IContactSearchResult contact)
    {
      contact.LatestVisitId = visit.InteractionId;
      contact.LatestVisitStartDateTime = visit.StartDateTime;
      contact.LatestVisitEndDateTime = visit.EndDateTime;
      contact.LatestVisitPageViewCount = visit.VisitPageCount;
      contact.LatestVisitValue = visit.Value;

      if (null != visit.WhoIs)
      {
        contact.LatestVisitLocationCityDisplayName = visit.WhoIs.City;
        contact.LatestVisitLocationCountryDisplayName = visit.WhoIs.Country;
        contact.LatestVisitLocationRegionDisplayName = visit.WhoIs.Region;
        contact.LatestVisitLocationId = visit.LocationId;
      }
    }

    #endregion

  }
}